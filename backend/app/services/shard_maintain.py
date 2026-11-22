"""分片维护归属领域服务。

背景：同一目标表可接收多条数据流（落盘管道）的写入，阻塞清理此前没有文件级
归属。本模块在内存仓储之上维护三类数据：

1. ``write_grant`` 写入授权：管道对目标表路径前缀的写权限，支持专有/共享；
2. ``shard_ownership`` 分片维护归属明细：阻塞批次逐文件登记，登记即做归属校验，
   形成供维护任务（小文件合并/分片重组）使用的归属明细；
3. ``maintain_batch`` 分片维护批次：管道对一批路径发起的重组申请，合并任务据此
   领取可操作对象、替换文件并按来源回传释放空间与仍阻塞路径。

关键不变量：
- 文件归属必须与允许发起写入的源端（管道）相符，越权登记不会进入任何维护批次；
- 共享文件保留每个授权管道的授权快照，任何时刻都能查出由哪条管道授权；
- 同一路径在任一时刻最多被一个维护批次领取，共享文件全局最多被替换一次；
- 撤销管道归属不删除历史：已确认的替换内容始终保留授权时快照，可追查。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.store import store


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _to_mb(value: Any) -> int:
    """把 8 / '8MB' / '8 MB' 统一换算成整数 MB。"""
    text = str(value if value is not None else "0").strip().upper().replace(" ", "")
    if text.endswith("MB"):
        text = text[:-2]
    return int(float(text or 0))


# 归属明细登记状态
REG_VALID = "登记有效"
REG_UNAUTHORIZED = "登记无效-越权"
REG_REVOKED = "授权已撤销"

# 归属明细替换状态
ST_PENDING = "待领取"
ST_CLAIMED = "已领取"
ST_REPLACED = "已替换"

# 维护批次状态
BATCH_APPLIED = "已申请"
BATCH_RUNNING = "领取中"
BATCH_DONE = "已完成"

# 共享 / 专有
SCOPE_EXCLUSIVE = "专有"
SCOPE_SHARED = "共享"


class ShardMaintainService:
    """分片维护归属领域服务（跨表领域，不继承通用 CRUD 基类）。"""

    def __init__(self) -> None:
        # 单进程内存仓储，用一把可重入锁保证「校验+登记+领取」的原子性，
        # 并发申请同一路径时串行化临界区，杜绝同一文件进入两个任务。
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 写入授权 write_grant
    # ------------------------------------------------------------------
    def register_grant(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记/更新管道对目标表的写入授权（含路径前缀与共享范围）。"""
        pipeline = str(values.get("管道编号", "")).strip()
        table = str(values.get("目标表", "")).strip()
        prefix = str(values.get("授权前缀", "")).strip()
        if not pipeline or not table or not prefix:
            raise ValueError("管道编号、目标表、授权前缀为必填项")
        scope = str(values.get("授权范围") or SCOPE_EXCLUSIVE).strip() or SCOPE_EXCLUSIVE
        shared_with = [
            str(item).strip()
            for item in (values.get("共享管道") or [])
            if str(item).strip()
        ]

        with self._lock:
            rows = store.rows("write_grant")
            existing = next(
                (r for r in rows
                 if r["管道编号"] == pipeline and r["目标表"] == table
                 and r["授权前缀"] == prefix),
                None,
            )
            if existing is not None:
                existing["授权范围"] = scope
                existing["共享管道"] = shared_with
                existing["授权状态"] = "生效中"
                existing["更新时间"] = _now()
                return existing

            grant = {
                "id": _next_id(rows),
                "授权编号": f"GR-{_next_id(rows):04d}",
                "管道编号": pipeline,
                "目标表": table,
                "授权前缀": prefix,
                "授权范围": scope,
                "共享管道": shared_with,
                "授权状态": "生效中",
                "登记时间": _now(),
                "更新时间": _now(),
                "status": "生效中",
                "pending": True,
                "abnormal": False,
            }
            rows.append(grant)
            return grant

    def revoke_grant(self, pipeline: str, table: str, *, prefix: str | None = None) -> dict[str, Any]:
        """撤销管道对目标表（可限定前缀）的写入授权。

        撤销只改变授权与归属状态，不删除任何记录：已替换文件保留授权时快照，
        未被领取的明细变为「授权已撤销」并从可操作对象中剔除。
        """
        pipeline = pipeline.strip()
        table = table.strip()
        revoked_grants: list[dict[str, Any]] = []
        revoked_rows: list[dict[str, Any]] = []
        unlocked_paths: set[str] = set()
        with self._lock:
            for grant in store.rows("write_grant"):
                if (grant["管道编号"] == pipeline and grant["目标表"] == table
                        and (prefix is None or grant["授权前缀"] == prefix)
                        and grant["授权状态"] == "生效中"):
                    grant["授权状态"] = "已撤销"
                    grant["status"] = "已撤销"
                    grant["更新时间"] = _now()
                    revoked_grants.append(grant)

            for row in store.rows("shard_ownership"):
                if (row["授权管道"] == pipeline and row["目标表"] == table
                        and row["登记状态"] == REG_VALID
                        and row["替换状态"] != ST_REPLACED
                        and (prefix is None or row["文件路径"].startswith(prefix))):
                    claimed = row["替换状态"] == ST_CLAIMED
                    row["登记状态"] = REG_REVOKED
                    row["status"] = REG_REVOKED
                    row["abnormal"] = True
                    # 已领取但未替换的文件释放路径锁，使其他授权管道可接管共享文件；
                    # 已替换文件不动，授权快照与替换证据永久保留。
                    if claimed:
                        row["替换状态"] = ST_PENDING
                        row["锁定批次编号"] = None
                        row["claim_token"] = None
                        unlocked_paths.add(row["文件路径"])
                    row["状态更新时间"] = _now()
                    revoked_rows.append(row)

            # 被撤权管道的批次已无法完成这些文件：把同路径上、由该管道批次持有的
            # 其他授权行（共享伙伴的归属行）一并释放，保证共享文件可被接管。
            if unlocked_paths:
                own_batch_nos = {
                    b["批次编号"] for b in store.rows("maintain_batch")
                    if b["申请管道"] == pipeline
                }
                for row in store.rows("shard_ownership"):
                    if (row["文件路径"] in unlocked_paths
                            and row.get("锁定批次编号") in own_batch_nos):
                        row["替换状态"] = ST_PENDING
                        row["锁定批次编号"] = None
                        row["claim_token"] = None
                        row["状态更新时间"] = _now()

        return {
            "ok": True,
            "撤销授权数": len(revoked_grants),
            "冻结归属明细数": len(revoked_rows),
            "撤销的授权": revoked_grants,
            "冻结的明细": revoked_rows,
            "说明": "已替换文件保留授权时快照不受影响；未领取明细已从可操作对象剔除",
        }

    def _active_grants(self, table: str) -> list[dict[str, Any]]:
        return [
            g for g in store.rows("write_grant")
            if g["目标表"] == table and g["授权状态"] == "生效中"
        ]

    def _authorize(
        self,
        pipeline: str,
        table: str,
        path: str,
        shared_partners: list[str] | None = None,
    ) -> tuple[dict[str, Any] | None, str, str, list[str]]:
        """校验某管道是否有权把路径登记进目标表的维护明细。

        授权分两层：
        - 前缀级：管道必须持有覆盖该路径前缀的生效写入授权，否则即越权；
        - 文件级：共享文件必须在登记时明示共享伙伴，且双方授权里互相包含
          （双向明示）。同一前缀下各自专有的文件只要不声明共享就保持专有。

        返回 (命中授权, 登记状态, 拒绝原因, 共享授权管道列表)。
        """
        grants = [
            g for g in self._active_grants(table)
            if path.startswith(g["授权前缀"])
        ]
        own = next((g for g in grants if g["管道编号"] == pipeline), None)
        if own is None:
            return None, REG_UNAUTHORIZED, f"管道{pipeline}无覆盖该路径前缀的生效授权", []

        partners = sorted({p.strip() for p in (shared_partners or []) if p.strip() and p != pipeline})
        if not partners:
            return own, REG_VALID, "", []

        for partner in partners:
            partner_grant = next((g for g in grants if g["管道编号"] == partner), None)
            if partner_grant is None:
                return (own, REG_UNAUTHORIZED,
                        f"共享声明失败：管道{partner}无覆盖该路径前缀的生效授权", [])
            if partner not in (own.get("共享管道") or []):
                return (own, REG_UNAUTHORIZED,
                        f"共享路径缺少管道{pipeline}对{partner}的明示共享授权", [])
            if pipeline not in (partner_grant.get("共享管道") or []):
                return (own, REG_UNAUTHORIZED,
                        f"共享路径缺少管道{partner}对{pipeline}的明示共享授权", [])
        return own, REG_VALID, "", sorted({pipeline, *partners})

    # ------------------------------------------------------------------
    # 分片维护归属明细 shard_ownership
    # ------------------------------------------------------------------
    def register_ownership(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记一个阻塞批次里的文件归属，并立即做文件级归属校验。"""
        pipeline = str(values.get("管道编号", "")).strip()
        table = str(values.get("目标表", "")).strip()
        path = str(values.get("文件路径", "")).strip()
        if not pipeline or not table or not path:
            raise ValueError("管道编号、目标表、文件路径为必填项")
        size_mb = _to_mb(values.get("文件大小MB", 0))
        block_batch = str(values.get("阻塞批次", "")).strip() or None
        cleanup_range = str(values.get("清理区间", "")).strip() or None
        declared_partners = [
            str(p).strip()
            for p in (values.get("共享授权管道") or [])
            if str(p).strip() and str(p).strip() != pipeline
        ]

        with self._lock:
            rows = store.rows("shard_ownership")
            # 同一管道对同一路径的重复登记幂等返回，避免重复进入维修任务。
            dup = next(
                (r for r in rows
                 if r["文件路径"] == path and r["授权管道"] == pipeline),
                None,
            )
            if dup is not None:
                dup["重复登记次数"] = int(dup.get("重复登记次数", 0)) + 1
                return dup

            # 文件级交叉校验：同一路径已存在其他管道的登记时，本次登记必须
            # 明示把对方列为共享伙伴，否则按越权拦截（不同清理区间也不例外）。
            existing_owners = sorted({
                r["授权管道"] for r in rows
                if r["文件路径"] == path and r["登记状态"] == REG_VALID
            })
            if existing_owners:
                missing = [p for p in existing_owners if p not in declared_partners]
                if missing:
                    reg_status = REG_UNAUTHORIZED
                    reason = (f"路径已由管道{'、'.join(missing)}登记且未对本管道明示共享授权，"
                              "同前缀不同清理区间不得覆盖他人专有文件")
                    grant, shared = None, []
                else:
                    grant, reg_status, reason, shared = self._authorize(
                        pipeline, table, path, declared_partners)
            else:
                grant, reg_status, reason, shared = self._authorize(
                    pipeline, table, path, declared_partners)

            scope = SCOPE_SHARED if shared else SCOPE_EXCLUSIVE
            row_id = _next_id(rows)
            row = {
                "id": row_id,
                "归属编号": f"SO-{row_id:04d}",
                "目标表": table,
                "文件路径": path,
                "文件大小MB": size_mb,
                "授权管道": pipeline,
                "授权范围": scope,
                "共享授权管道": shared,
                "授权编号": grant["授权编号"] if grant else None,
                # 授权快照：调整/撤销管道归属后，已确认替换仍可凭快照追查。
                "授权快照": {
                    "授权编号": grant["授权编号"] if grant else None,
                    "授权前缀": grant["授权前缀"] if grant else None,
                    "授权范围": grant["授权范围"] if grant else None,
                    "登记时共享管道": list(shared),
                    "登记时间": _now(),
                },
                "阻塞批次": block_batch,
                "清理区间": cleanup_range,
                "登记状态": reg_status,
                "替换状态": ST_PENDING if reg_status == REG_VALID else "不可领取",
                "锁定批次编号": None,
                "claim_token": None,
                "替换时间": None,
                "替换证据": None,
                "拒绝原因": reason or None,
                "重复登记次数": 0,
                "状态更新时间": _now(),
                "status": reg_status,
                "pending": reg_status == REG_VALID,
                "abnormal": reg_status != REG_VALID,
            }
            rows.append(row)

            # 共享登记成功：把该路径既有授权行同步升级为共享，保证同一路径的
            # 全部归属行共享同一份授权管道集合（可查出任意一条管道的授权来源）。
            if reg_status == REG_VALID and scope == SCOPE_SHARED:
                for prior in rows:
                    if (prior["文件路径"] == path and prior["id"] != row_id
                            and prior["登记状态"] == REG_VALID):
                        prior["授权范围"] = SCOPE_SHARED
                        prior["共享授权管道"] = list(shared)
                        prior["授权快照"]["登记时共享管道"] = list(shared)
                        prior["状态更新时间"] = _now()
            return row

    def ownership_detail(self, *, table: str | None = None, path: str | None = None) -> list[dict[str, Any]]:
        """查询分片维护归属明细；按路径查时可看到共享文件的每条授权来源。"""
        with self._lock:
            rows = list(store.rows("shard_ownership"))
        if table:
            rows = [r for r in rows if r["目标表"] == table]
        if path:
            rows = [r for r in rows if r["文件路径"] == path]
        return rows

    # ------------------------------------------------------------------
    # 分片维护批次 maintain_batch
    # ------------------------------------------------------------------
    def apply_maintenance(self, values: dict[str, Any]) -> dict[str, Any]:
        """管道对一批路径申请分片重组（维护任务）。

        只接受归属校验通过且仍在授权期内的明细；越权登记在入口即被剔除，
        不可能进入维修任务。返回受理/拒绝明细，便于调用方对照原始清单。
        """
        pipeline = str(values.get("管道编号", "")).strip()
        table = str(values.get("目标表", "")).strip()
        paths = [str(p).strip() for p in (values.get("路径清单") or []) if str(p).strip()]
        if not pipeline or not table or not paths:
            raise ValueError("管道编号、目标表、路径清单为必填项")

        with self._lock:
            batch_rows = store.rows("maintain_batch")
            batch_id = _next_id(batch_rows)
            batch_no = f"MB-{batch_id:04d}"
            accepted: list[dict[str, Any]] = []
            rejected: list[dict[str, Any]] = []

            for path in paths:
                row = next(
                    (r for r in store.rows("shard_ownership")
                     if r["文件路径"] == path and r["授权管道"] == pipeline),
                    None,
                )
                if row is None:
                    rejected.append({"文件路径": path, "原因": "未找到该管道的归属登记，请先登记分片归属"})
                    continue
                if row["登记状态"] == REG_UNAUTHORIZED:
                    rejected.append({"文件路径": path, "原因": f"越权登记不得进入维修任务（{row.get('拒绝原因')}）"})
                    continue
                if row["登记状态"] == REG_REVOKED:
                    rejected.append({"文件路径": path, "原因": "管道授权已撤销"})
                    continue
                if row["替换状态"] != ST_PENDING or row["锁定批次编号"] is not None:
                    rejected.append({
                        "文件路径": path,
                        "原因": f"路径已被批次{row['锁定批次编号']}领取/替换，不可重复进入任务",
                    })
                    continue
                accepted.append(row)

            # 关联该表现存的「合并中」小文件合并任务，后续释放空间回写到任务上。
            linked_compaction = next(
                (c for c in store.rows("compaction")
                 if str(c.get("目标表")) == table and c.get("status") == "合并中"),
                None,
            )
            batch = {
                "id": batch_id,
                "批次编号": batch_no,
                "目标表": table,
                "申请管道": pipeline,
                "原始路径清单": list(paths),
                "受理路径": [r["文件路径"] for r in accepted],
                "拒绝路径": rejected,
                "已替换路径": [],
                "批次状态": BATCH_APPLIED,
                "claim_token": None,
                "关联合并任务": linked_compaction.get("任务编号") if linked_compaction else None,
                "申请时间": _now(),
                "status": BATCH_APPLIED,
                "pending": True,
                "abnormal": False,
            }
            batch_rows.append(batch)
            return {
                "ok": True,
                "批次": batch,
                "受理数": len(accepted),
                "拒绝数": len(rejected),
                "拒绝明细": rejected,
            }

    def _get_batch(self, batch_id: int) -> dict[str, Any] | None:
        return store.find("maintain_batch", batch_id)

    def claim_objects(self, batch_id: int) -> dict[str, Any]:
        """合并任务按归属明细领取可操作对象并加路径级锁。

        临界区内再次校验授权与锁定状态：并发申请下，同一路径只有第一个批次能
        领取成功，其余批次在该路径上得到冲突说明，保证同一路径不会在两个任务
        里都可替换。
        """
        with self._lock:
            batch = self._get_batch(batch_id)
            if batch is None:
                return {"ok": False, "message": f"维护批次 {batch_id} 不存在"}
            if batch["批次状态"] == BATCH_DONE:
                return {"ok": False, "message": "批次已完成，不能重复领取"}

            token = batch["claim_token"] or f"claim-{batch['批次编号']}-{_now()}"
            batch["claim_token"] = token
            batch["批次状态"] = BATCH_RUNNING
            batch["status"] = BATCH_RUNNING

            actionable: list[dict[str, Any]] = []
            blocked: list[dict[str, Any]] = []
            ownership_rows = store.rows("shard_ownership")

            for path in batch["受理路径"]:
                # 一个批次对同一路径只关心本管道的归属行；共享文件的另一管道行
                # 通过路径级锁联动处理。
                rows_for_path = [r for r in ownership_rows if r["文件路径"] == path]
                own_row = next(
                    (r for r in rows_for_path if r["授权管道"] == batch["申请管道"]), None)
                if own_row is None:
                    blocked.append({"文件路径": path, "原因": "归属登记缺失"})
                    continue
                if own_row["登记状态"] == REG_REVOKED:
                    blocked.append({"文件路径": path, "原因": "授权已撤销（申请后权限被调整）"})
                    continue
                if own_row["替换状态"] == ST_REPLACED:
                    blocked.append({"文件路径": path, "原因": "共享文件已被其他批次替换，全局最多替换一次"})
                    continue
                locker = own_row.get("锁定批次编号")
                if locker is not None and locker != batch["批次编号"]:
                    blocked.append({"文件路径": path, "原因": f"路径已被并发批次{locker}锁定"})
                    continue
                if own_row["替换状态"] == ST_CLAIMED:
                    # 本批次重复领取：回显已持有的可操作对象，不重复加锁。
                    actionable.append({
                        "归属编号": own_row["归属编号"],
                        "文件路径": path,
                        "文件大小MB": own_row["文件大小MB"],
                        "授权管道": own_row["授权管道"],
                        "授权范围": own_row["授权范围"],
                        "共享授权管道": own_row["共享授权管道"],
                        "清理区间": own_row.get("清理区间"),
                    })
                    continue

                # 领取成功：对该路径的全部有效归属行（含共享的其他授权管道行）
                # 一并加锁，确保共享文件不会被第二个批次再次领取。
                for r in rows_for_path:
                    if r["登记状态"] == REG_VALID and r["替换状态"] == ST_PENDING:
                        r["替换状态"] = ST_CLAIMED
                        r["锁定批次编号"] = batch["批次编号"]
                        r["claim_token"] = token
                        r["状态更新时间"] = _now()
                actionable.append({
                    "归属编号": own_row["归属编号"],
                    "文件路径": path,
                    "文件大小MB": own_row["文件大小MB"],
                    "授权管道": own_row["授权管道"],
                    "授权范围": own_row["授权范围"],
                    "共享授权管道": own_row["共享授权管道"],
                    "清理区间": own_row.get("清理区间"),
                })

            return {
                "ok": True,
                "批次编号": batch["批次编号"],
                "claim_token": token,
                "可操作对象": actionable,
                "仍阻塞路径": blocked,
                "可操作文件数": len(actionable),
                "阻塞文件数": len(blocked),
            }

    def confirm_replacement(
        self, batch_id: int, paths: list[str], *, replacement: str | None = None
    ) -> dict[str, Any]:
        """确认一批文件已被合并任务替换，落替换证据并按来源记录释放空间。"""
        paths = [p.strip() for p in paths if str(p).strip()]
        with self._lock:
            batch = self._get_batch(batch_id)
            if batch is None:
                return {"ok": False, "message": f"维护批次 {batch_id} 不存在"}
            replaced_now: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            for path in paths:
                rows_for_path = [
                    r for r in store.rows("shard_ownership") if r["文件路径"] == path]
                own_row = next(
                    (r for r in rows_for_path if r["授权管道"] == batch["申请管道"]), None)
                if own_row is None or own_row.get("锁定批次编号") != batch["批次编号"]:
                    skipped.append({"文件路径": path, "原因": "未被本批次锁定"})
                    continue
                if own_row["替换状态"] == ST_REPLACED:
                    skipped.append({"文件路径": path, "原因": "共享文件已替换过，跳过"})
                    continue

                evidence = replacement or f"s3://warehouse/rewritten/{batch['批次编号']}/{path.rsplit('/', 1)[-1]}"
                ts = _now()
                # 同路径的所有授权行一起标记已替换（共享文件只替换一次，但每条
                # 授权管道都保留可追查的替换证据与授权快照）。
                for r in rows_for_path:
                    if r.get("锁定批次编号") == batch["批次编号"]:
                        r["替换状态"] = ST_REPLACED
                        r["替换时间"] = ts
                        r["替换证据"] = evidence
                        r["状态更新时间"] = ts
                        r["pending"] = False
                if path not in batch["已替换路径"]:
                    batch["已替换路径"].append(path)
                replaced_now.append(own_row)

            # 受理路径全部替换完成则收尾批次。
            outstanding = [
                p for p in batch["受理路径"]
                if p not in batch["已替换路径"]
            ]
            if not outstanding:
                batch["批次状态"] = BATCH_DONE
                batch["status"] = BATCH_DONE
                batch["pending"] = False

            self._sync_compaction(batch)
            return {
                "ok": True,
                "批次编号": batch["批次编号"],
                "本次替换": [r["文件路径"] for r in replaced_now],
                "跳过": skipped,
                "批次状态": batch["批次状态"],
                "累计已替换": list(batch["已替换路径"]),
            }

    def _sync_compaction(self, batch: dict[str, Any]) -> None:
        """把本批次按来源释放空间与仍阻塞路径回写到关联的小文件合并任务。"""
        comp = next(
            (c for c in store.rows("compaction")
             if c.get("任务编号") == batch.get("关联合并任务")),
            None,
        )
        if comp is None:
            return
        report = self.source_report(batch["目标表"])
        comp["来源释放空间MB"] = report["按来源释放空间MB"]
        comp["仍阻塞路径"] = report["仍阻塞路径"]
        comp["归属批次编号"] = batch["批次编号"]
        total_mb = sum(item["释放空间MB"] for item in report["按来源释放空间MB"])
        comp["释放空间"] = f"{total_mb}MB" if total_mb else comp.get("释放空间")

    # ------------------------------------------------------------------
    # 对账与回传
    # ------------------------------------------------------------------
    def source_report(self, table: str) -> dict[str, Any]:
        """按来源（管道）回传实际释放空间，并列出仍阻塞路径。

        共享文件的释放空间按替换时点的全部有效授权管道均摊；每个来源都能看到
        自己专有与共享两部分明细。
        """
        with self._lock:
            rows = [r for r in store.rows("shard_ownership") if r["目标表"] == table]
            per_source: dict[str, dict[str, Any]] = {}

            def ensure(pipeline: str) -> dict[str, Any]:
                return per_source.setdefault(pipeline, {
                    "管道编号": pipeline,
                    "专有释放MB": 0,
                    "共享释放MB": 0,
                    "释放空间MB": 0,
                    "替换文件": [],
                })

            # 已替换文件按路径去重后再分摊，共享文件只算一次物理释放。
            replaced_paths = sorted({r["文件路径"] for r in rows if r["替换状态"] == ST_REPLACED})
            for path in replaced_paths:
                # 只认真实授权行：越权无效登记不构成授权来源，不参与空间分摊。
                path_rows = [
                    r for r in rows
                    if r["文件路径"] == path and r["登记状态"] != REG_UNAUTHORIZED
                ]
                if not path_rows:
                    continue
                size_mb = int(path_rows[0]["文件大小MB"])
                # 授权管道取「登记时授权快照」里的共享集合，撤权后仍可对账。
                authorizers = sorted({
                    p
                    for r in path_rows
                    for p in (r.get("授权快照", {}).get("登记时共享管道") or [r["授权管道"]])
                })
                share = size_mb // len(authorizers) if authorizers else size_mb
                remainder = size_mb - share * len(authorizers)
                is_shared = (
                    len(authorizers) > 1
                    or any(r["授权范围"] == SCOPE_SHARED for r in path_rows)
                )
                for idx, pipeline in enumerate(authorizers):
                    bucket = ensure(pipeline)
                    allocated = share + (remainder if idx == 0 else 0)
                    if is_shared:
                        bucket["共享释放MB"] += allocated
                    else:
                        bucket["专有释放MB"] += allocated
                    bucket["释放空间MB"] += allocated
                    bucket["替换文件"].append({
                        "文件路径": path,
                        "分摊释放MB": allocated,
                        "授权范围": SCOPE_SHARED if is_shared else SCOPE_EXCLUSIVE,
                    })

            blocked: list[dict[str, Any]] = []
            for r in rows:
                if r["登记状态"] == REG_UNAUTHORIZED:
                    blocked.append({"文件路径": r["文件路径"], "授权管道": r["授权管道"],
                                    "原因": r.get("拒绝原因") or "越权登记"})
                elif r["登记状态"] == REG_REVOKED and r["替换状态"] != ST_REPLACED:
                    blocked.append({"文件路径": r["文件路径"], "授权管道": r["授权管道"],
                                    "原因": "授权已撤销且文件尚未替换"})
            # 去重（共享路径可能多行）。
            seen: set[tuple[str, str]] = set()
            uniq_blocked: list[dict[str, Any]] = []
            for item in blocked:
                key = (item["文件路径"], item["原因"])
                if key not in seen:
                    seen.add(key)
                    uniq_blocked.append(item)

            return {
                "目标表": table,
                "按来源释放空间MB": list(per_source.values()),
                "总释放空间MB": sum(b["释放空间MB"] for b in per_source.values()),
                "仍阻塞路径": uniq_blocked,
            }

    def reconcile(self, batch_id: int) -> dict[str, Any]:
        """按原始申请清单与实际已替换文件逐项对照，输出对账明细。"""
        with self._lock:
            batch = self._get_batch(batch_id)
            if batch is None:
                return {"ok": False, "message": f"维护批次 {batch_id} 不存在"}
            rows = store.rows("shard_ownership")
            items: list[dict[str, Any]] = []
            for path in batch["原始路径清单"]:
                own_row = next(
                    (r for r in rows
                     if r["文件路径"] == path and r["授权管道"] == batch["申请管道"]),
                    None,
                )
                if own_row is None:
                    items.append({"文件路径": path, "对账结果": "未登记归属", "文件大小MB": None})
                    continue
                if own_row["替换状态"] == ST_REPLACED:
                    result = "已替换"
                elif own_row["登记状态"] == REG_REVOKED:
                    result = "未替换-授权已撤销"
                elif own_row["登记状态"] == REG_UNAUTHORIZED:
                    result = "未替换-越权登记"
                elif own_row["替换状态"] == ST_CLAIMED:
                    result = "已领取待替换"
                else:
                    result = "未替换"
                items.append({
                    "文件路径": path,
                    "文件大小MB": own_row["文件大小MB"],
                    "授权范围": own_row["授权范围"],
                    "共享授权管道": own_row["共享授权管道"],
                    "对账结果": result,
                    "替换证据": own_row.get("替换证据"),
                    "授权快照": own_row.get("授权快照"),
                })

            replaced_mb = sum(
                i["文件大小MB"] for i in items
                if i["对账结果"] == "已替换" and i["文件大小MB"] is not None
            )
            pending_mb = sum(
                i["文件大小MB"] for i in items
                if i["对账结果"] != "已替换" and i["文件大小MB"] is not None
            )
            return {
                "ok": True,
                "批次编号": batch["批次编号"],
                "申请管道": batch["申请管道"],
                "目标表": batch["目标表"],
                "原始清单数": len(batch["原始路径清单"]),
                "实际已替换数": len(batch["已替换路径"]),
                "已替换空间MB": replaced_mb,
                "尚未替换空间MB": pending_mb,
                "对账明细": items,
            }


shard_maintain = ShardMaintainService()
