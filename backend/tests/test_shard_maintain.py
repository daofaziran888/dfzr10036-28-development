"""分片维护归属明细：端到端场景测试（标准库 unittest，无需额外依赖）。

覆盖需求叙述：
1. 同一目标表接收多条数据流落盘，阻塞清理具备文件级归属校验；
2. 管道越权注册的路径不能进入维修任务；
3. 共享文件可查出由哪条管道授权（双向明示共享授权）；
4. 调整管道归属后，已确认替换内容保持可追查（授权快照不删除）；
5. 并发申请不能使同一路径在两个任务里都可替换（路径级锁 + 共享只替换一次）；
6. 小文件合并依据归属明细领取可操作对象，回传每个来源实际释放空间与仍阻塞路径；
7. A 专有 p1=8MB、B 专有 p2=13MB、p3 双方明示共享；先完成 p1，撤 A 权限后
   再查看可操作对象；按原始清单与实际已替换文件对照 A 的 8MB 与 B 未替换范围。

运行：backend/.venv/bin/python -m unittest tests.test_shard_maintain -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

# 让 tests 目录独立运行时也能 import 到 app。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.shard_maintain import (  # noqa: E402
    BATCH_DONE,
    REG_REVOKED,
    REG_UNAUTHORIZED,
    REG_VALID,
    SCOPE_EXCLUSIVE,
    SCOPE_SHARED,
    ST_REPLACED,
    shard_maintain,
)
from app.store import store  # noqa: E402

TABLE = "ads.orders_iceberg"
PIPE_A = "SK-PIPE-A"
PIPE_B = "SK-PIPE-B"
PIPE_C = "SK-PIPE-C"  # 无授权管道，用于越权注册
PREFIX = "s3://warehouse/ads/orders/ds=2026-11-22/"
P1 = f"{PREFIX}p1.parquet"
P2 = f"{PREFIX}p2.parquet"
P3 = f"{PREFIX}p3.parquet"
P4 = f"{PREFIX}p4.parquet"  # A 专有，申请后、替换前撤权 -> 仍阻塞
P5 = f"{PREFIX}p5.parquet"  # B 专有，全程未替换 -> 对账体现未替换范围


def reset_state() -> None:
    store.reset()
    # 授权：A、B 各有专有权限；共享前缀由双方互相明示授权。
    shard_maintain.register_grant({
        "管道编号": PIPE_A, "目标表": TABLE,
        "授权前缀": PREFIX, "授权范围": "专有", "共享管道": [PIPE_B],
    })
    shard_maintain.register_grant({
        "管道编号": PIPE_B, "目标表": TABLE,
        "授权前缀": PREFIX, "授权范围": "专有", "共享管道": [PIPE_A],
    })


def register(path: str, pipeline: str, size_mb: int, *, batch: str, cleanup: str,
             partners: list[str] | None = None) -> dict:
    return shard_maintain.register_ownership({
        "管道编号": pipeline, "目标表": TABLE, "文件路径": path,
        "文件大小MB": size_mb, "阻塞批次": batch, "清理区间": cleanup,
        "共享授权管道": partners or [],
    })


class ShardMaintainScenarioTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_state()

    # 1. 文件级归属校验 + 共享授权识别 ------------------------------
    def test_ownership_validation_and_shared_scope(self) -> None:
        r1 = register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        r2 = register(P2, PIPE_B, 13, batch="BLK-B-1", cleanup="range-B")
        r3a = register(P3, PIPE_A, 20, batch="BLK-A-1", cleanup="range-A", partners=[PIPE_B])
        r3b = register(P3, PIPE_B, 20, batch="BLK-B-1", cleanup="range-B", partners=[PIPE_A])

        self.assertEqual(r1["登记状态"], REG_VALID)
        self.assertEqual(r1["授权范围"], SCOPE_EXCLUSIVE)
        self.assertEqual(r2["登记状态"], REG_VALID)
        self.assertEqual(r3a["登记状态"], REG_VALID)
        # 同前缀双方都有生效授权且互相明示 => 识别为共享。
        self.assertEqual(r3a["授权范围"], SCOPE_SHARED)
        self.assertEqual(r3b["授权范围"], SCOPE_SHARED)
        self.assertEqual(sorted(r3a["共享授权管道"]), [PIPE_A, PIPE_B])

        # 共享文件必须能查出它由哪条管道授权：两条归属记录都在。
        detail = shard_maintain.ownership_detail(table=TABLE, path=P3)
        self.assertEqual({r["授权管道"] for r in detail}, {PIPE_A, PIPE_B})
        for row in detail:
            self.assertEqual(row["授权快照"]["授权前缀"], PREFIX)
            self.assertIn("登记时间", row["授权快照"])

    # 2. 越权注册不能进入维修任务 -----------------------------------
    def test_unauthorized_registration_blocked(self) -> None:
        bad = register(P1, PIPE_C, 8, batch="BLK-C-1", cleanup="range-C")
        self.assertEqual(bad["登记状态"], REG_UNAUTHORIZED)
        self.assertTrue(bad["abnormal"])
        self.assertIsNotNone(bad["拒绝原因"])
        self.assertEqual(bad["替换状态"], "不可领取")

        applied = shard_maintain.apply_maintenance({
            "管道编号": PIPE_C, "目标表": TABLE, "路径清单": [P1],
        })
        self.assertEqual(applied["受理数"], 0)
        self.assertEqual(applied["拒绝数"], 1)
        self.assertIn("越权", applied["拒绝明细"][0]["原因"])
        # 维修批次里没有任何受理路径，越权文件不可能被替换。
        self.assertEqual(applied["批次"]["受理路径"], [])

    # 3. 并行申请重组：同前缀、不同清理区间，路径级互斥 -------------
    def test_parallel_apply_and_path_level_lock(self) -> None:
        register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        register(P2, PIPE_B, 13, batch="BLK-B-1", cleanup="range-B")
        register(P3, PIPE_A, 20, batch="BLK-A-1", cleanup="range-A", partners=[PIPE_B])
        register(P3, PIPE_B, 20, batch="BLK-B-1", cleanup="range-B", partners=[PIPE_A])

        batch_a = shard_maintain.apply_maintenance({
            "管道编号": PIPE_A, "目标表": TABLE, "路径清单": [P1, P3],
        })
        batch_b = shard_maintain.apply_maintenance({
            "管道编号": PIPE_B, "目标表": TABLE, "路径清单": [P2, P3],
        })
        id_a = batch_a["批次"]["id"]
        id_b = batch_b["批次"]["id"]
        self.assertEqual(batch_a["批次"]["受理路径"], [P1, P3])
        self.assertEqual(batch_b["批次"]["受理路径"], [P2, P3])

        # A 先领取：p1 与共享 p3 被 A 批次锁定（p3 的两条归属行一起锁）。
        claim_a = shard_maintain.claim_objects(id_a)
        self.assertEqual(
            sorted(o["文件路径"] for o in claim_a["可操作对象"]), [P1, P3])
        self.assertEqual(claim_a["仍阻塞路径"], [])

        # B 再领取：p2 可领取，p3 已被并发批次锁定，不能在两个任务里都可替换。
        claim_b = shard_maintain.claim_objects(id_b)
        self.assertEqual(
            [o["文件路径"] for o in claim_b["可操作对象"]], [P2])
        self.assertEqual(len(claim_b["仍阻塞路径"]), 1)
        self.assertEqual(claim_b["仍阻塞路径"][0]["文件路径"], P3)
        self.assertIn("锁定", claim_b["仍阻塞路径"][0]["原因"])

        # 存储层校验：p3 的两条归属行锁定批次都是 A。
        p3_rows = [r for r in store.rows("shard_ownership") if r["文件路径"] == P3]
        self.assertTrue(all(r["锁定批次编号"] == batch_a["批次"]["批次编号"]
                            for r in p3_rows))

    # 4. 撤权后已替换内容可追查 + 可操作对象重算 + 仍阻塞回传 ------
    def test_revoke_keeps_replaced_traceable_and_refreezes_pending(self) -> None:
        register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        register(P2, PIPE_B, 13, batch="BLK-B-1", cleanup="range-B")
        register(P3, PIPE_A, 20, batch="BLK-A-1", cleanup="range-A", partners=[PIPE_B])
        register(P3, PIPE_B, 20, batch="BLK-B-1", cleanup="range-B", partners=[PIPE_A])
        register(P4, PIPE_A, 5, batch="BLK-A-1", cleanup="range-A")

        batch_a = shard_maintain.apply_maintenance({
            "管道编号": PIPE_A, "目标表": TABLE, "路径清单": [P1, P3, P4],
        })
        batch_b = shard_maintain.apply_maintenance({
            "管道编号": PIPE_B, "目标表": TABLE, "路径清单": [P2, P3],
        })
        id_a = batch_a["批次"]["id"]
        id_b = batch_b["批次"]["id"]

        # A 先领取并只完成 p1（其余未替换）。
        shard_maintain.claim_objects(id_a)
        done = shard_maintain.confirm_replacement(id_a, [P1])
        self.assertIn(P1, done["累计已替换"])
        self.assertNotEqual(done["批次状态"], BATCH_DONE)  # 还有未替换路径

        # 趁其余未替换撤掉 A 的权限。
        revoke = shard_maintain.revoke_grant(PIPE_A, TABLE)
        self.assertGreaterEqual(revoke["冻结归属明细数"], 1)

        # p3、p4 的 A 归属行被冻结；p1 已替换，状态不动、证据保留。
        rows_by_path = {
            r["文件路径"]: r for r in store.rows("shard_ownership")
            if r["授权管道"] == PIPE_A
        }
        self.assertEqual(rows_by_path[P1]["替换状态"], ST_REPLACED)
        self.assertIsNotNone(rows_by_path[P1]["替换证据"])
        self.assertEqual(rows_by_path[P1]["授权快照"]["登记时共享管道"], [])
        self.assertEqual(rows_by_path[P3]["登记状态"], REG_REVOKED)
        self.assertEqual(rows_by_path[P4]["登记状态"], REG_REVOKED)

        # 查看 B 的合并任务可操作对象：B 专有 p2 与共享 p3（B 授权仍有效）。
        # 撤权已自动释放 A 对 p3 的未替换领取锁，B 可直接接管共享文件。
        claim_b = shard_maintain.claim_objects(id_b)
        actionable_paths = sorted(o["文件路径"] for o in claim_b["可操作对象"])
        self.assertEqual(actionable_paths, [P2, P3])

        # 来源回传：A 已实际释放 8MB（p1），p4 因撤权未替换出现在仍阻塞路径。
        report = shard_maintain.source_report(TABLE)
        a_bucket = next(b for b in report["按来源释放空间MB"] if b["管道编号"] == PIPE_A)
        self.assertEqual(a_bucket["专有释放MB"], 8)
        blocked_paths = {item["文件路径"] for item in report["仍阻塞路径"]}
        self.assertIn(P4, blocked_paths)

    # 5. 同一份共享文件最多被替换一次 + 释放空间按来源分摊 ---------
    def test_shared_file_replaced_once_and_space_split(self) -> None:
        register(P2, PIPE_B, 13, batch="BLK-B-1", cleanup="range-B")
        register(P3, PIPE_A, 20, batch="BLK-A-1", cleanup="range-A", partners=[PIPE_B])
        register(P3, PIPE_B, 20, batch="BLK-B-1", cleanup="range-B", partners=[PIPE_A])

        batch_b = shard_maintain.apply_maintenance({
            "管道编号": PIPE_B, "目标表": TABLE, "路径清单": [P2, P3],
        })
        id_b = batch_b["批次"]["id"]
        shard_maintain.claim_objects(id_b)
        first = shard_maintain.confirm_replacement(id_b, [P2, P3])
        self.assertEqual(sorted(first["本次替换"]), [P2, P3])

        # 重复替换 p3：直接跳过，不会产生第二次替换。
        again = shard_maintain.confirm_replacement(id_b, [P3])
        self.assertEqual(again["本次替换"], [])
        self.assertEqual(again["跳过"][0]["文件路径"], P3)

        # 物理上 p3 只有一份，按两个授权来源均摊 20MB = 10 + 10；
        # p2=13MB 计入 B 的专有释放。
        report = shard_maintain.source_report(TABLE)
        buckets = {b["管道编号"]: b for b in report["按来源释放空间MB"]}
        self.assertEqual(buckets[PIPE_A]["共享释放MB"], 10)
        self.assertEqual(buckets[PIPE_B]["专有释放MB"], 13)
        self.assertEqual(buckets[PIPE_B]["共享释放MB"], 10)
        self.assertEqual(buckets[PIPE_B]["释放空间MB"], 23)
        self.assertEqual(report["总释放空间MB"], 33)  # 13 + 20，共享只计一次物理释放

    # 6. 原始清单 vs 实际已替换对账：A 的 8MB 与 B 未替换范围 -------
    def test_reconcile_original_vs_actual(self) -> None:
        register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        register(P4, PIPE_A, 5, batch="BLK-A-1", cleanup="range-A")
        register(P2, PIPE_B, 13, batch="BLK-B-1", cleanup="range-B")
        register(P5, PIPE_B, 9, batch="BLK-B-1", cleanup="range-B")

        batch_a = shard_maintain.apply_maintenance({
            "管道编号": PIPE_A, "目标表": TABLE, "路径清单": [P1, P4],
        })
        batch_b = shard_maintain.apply_maintenance({
            "管道编号": PIPE_B, "目标表": TABLE, "路径清单": [P2, P5],
        })
        id_a, id_b = batch_a["批次"]["id"], batch_b["批次"]["id"]

        shard_maintain.claim_objects(id_a)
        shard_maintain.confirm_replacement(id_a, [P1])  # A 只完成 p1
        shard_maintain.revoke_grant(PIPE_A, TABLE)      # 随后撤权

        rec_a = shard_maintain.reconcile(id_a)
        self.assertEqual(rec_a["已替换空间MB"], 8)      # A 的 8MB 全部已替换
        self.assertEqual(rec_a["尚未替换空间MB"], 5)    # p4 撤权未替换
        results_a = {i["文件路径"]: i["对账结果"] for i in rec_a["对账明细"]}
        self.assertEqual(results_a[P1], "已替换")
        self.assertEqual(results_a[P4], "未替换-授权已撤销")

        # B 尚未做任何替换：13MB + 9MB 全部落在未替换范围。
        rec_b = shard_maintain.reconcile(id_b)
        self.assertEqual(rec_b["已替换空间MB"], 0)
        self.assertEqual(rec_b["尚未替换空间MB"], 22)
        self.assertTrue(all(i["对账结果"] == "未替换" for i in rec_b["对账明细"]))

    # 7. 并发申请不能使同一路径在两个任务里都可替换（重复申请拦截） -
    def test_concurrent_apply_same_path_rejected_second_time(self) -> None:
        register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        first = shard_maintain.apply_maintenance({
            "管道编号": PIPE_A, "目标表": TABLE, "路径清单": [P1],
        })
        shard_maintain.claim_objects(first["批次"]["id"])

        second = shard_maintain.apply_maintenance({
            "管道编号": PIPE_A, "目标表": TABLE, "路径清单": [P1],
        })
        self.assertEqual(second["受理数"], 0)
        self.assertIn("已被批次", second["拒绝明细"][0]["原因"])
        # 全局只有一个批次锁着 p1。
        lockers = {
            r["锁定批次编号"] for r in store.rows("shard_ownership")
            if r["文件路径"] == P1 and r["锁定批次编号"]
        }
        self.assertEqual(len(lockers), 1)

    # 8. 同前缀不同清理区间：未明示共享不得覆盖他人专有文件 ---------
    def test_same_prefix_different_range_requires_explicit_share(self) -> None:
        # A 先把 p1 登记为自己专有（清理区间 range-A）。
        register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        # B 虽持有同前缀授权、且有自己的清理区间，但未明示共享即登记同路径 => 越权。
        overreach = register(P1, PIPE_B, 8, batch="BLK-B-1", cleanup="range-B")
        self.assertEqual(overreach["登记状态"], REG_UNAUTHORIZED)
        self.assertIn("未对本管道明示共享授权", overreach["拒绝原因"])
        # 越权登记的行不能被 B 的维护批次受理。
        applied = shard_maintain.apply_maintenance({
            "管道编号": PIPE_B, "目标表": TABLE, "路径清单": [P1],
        })
        self.assertEqual(applied["受理数"], 0)
        # 原专有行不受影响，仍只归属 A。
        p1_rows = [r for r in store.rows("shard_ownership") if r["文件路径"] == P1]
        self.assertEqual(len(p1_rows), 2)  # 一条 A 有效 + 一条 B 越权留痕
        valid = [r for r in p1_rows if r["登记状态"] == REG_VALID]
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0]["授权管道"], PIPE_A)
        self.assertEqual(valid[0]["授权范围"], SCOPE_EXCLUSIVE)

    # 9. 重复登记幂等 ----------------------------------------------
    def test_duplicate_registration_is_idempotent(self) -> None:
        first = register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        again = register(P1, PIPE_A, 8, batch="BLK-A-1", cleanup="range-A")
        self.assertEqual(first["id"], again["id"])
        self.assertEqual(again["重复登记次数"], 1)
        self.assertEqual(
            len([r for r in store.rows("shard_ownership") if r["文件路径"] == P1]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
