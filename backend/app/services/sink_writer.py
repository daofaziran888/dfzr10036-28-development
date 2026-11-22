"""结果落盘服务：写入完成后触发 iceberg_commit 原子提交和 data_diff 比对。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SinkWriterService(BaseModuleService):
    MODULE = "sink_writer"
    ENTITY = "落盘管道"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["管道编号", "数据流", "目标载体", "写入模式", "分区列", "批次大小", "压缩格式", "写入速率"]
    REQUIRED_FIELDS = ["管道编号", "数据流", "目标载体"]
    STATUS_ORDER = ["草稿", "写入中", "写入阻塞", "已停止"]
    ACTION_RULES = {"启动写入": "写入中", "解除阻塞": "写入阻塞", "停止写入": "已停止"}
    NEGATIVE_ACTIONS = []

    def trigger_iceberg_commit(self, entry_id: int) -> dict[str, Any] | None:
        """落盘管道写入中时，触发 iceberg_commit 原子提交快照。

        数据流链路: sink_writer -> iceberg_commit
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入中":
            return None

        commit_rows = store.rows("iceberg_commit")
        new_id = max((int(r.get("id", 0)) for r in commit_rows), default=0) + 1
        commit = {
            "id": new_id,
            "快照编号": f"ICEB-{new_id:04d}",
            "目标表": str(entry.get("目标载体", "unknown")),
            "数据文件": f"s3://warehouse/data/{entry.get('管道编号', 'default')}/part-00001.parquet",
            "清单文件": f"metadata/manifest-{new_id}.avro",
            "快照ID": f"{new_id * 1000 + 500}",
            "提交状态": "待提交",
            "影响行数": 128340,
            "提交时间": "2026-11-18 10:32:45",
            "status": "提交中",
            "pending": True,
            "abnormal": False,
        }
        commit_rows.append(commit)
        return commit

    def trigger_block_cleanup(
        self, entry_id: int, files: list[dict[str, Any]] | None = None
    ) -> dict[str, Any] | None:
        """写入阻塞时触发 compaction 合并小文件。

        数据流链路: sink_writer(写入阻塞) -> compaction
        阻塞批次携带文件清单时，先逐文件做归属校验，形成分片维护归属明细，
        越权文件会被拦截并在返回中明示，不会进入后续维修任务。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入阻塞":
            return None

        comp_rows = store.rows("compaction")
        new_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
        block_batch = f"BLK-{entry.get('管道编号', 'X')}-{new_id:04d}"
        comp = {
            "id": new_id,
            "任务编号": f"COMP-{new_id:04d}",
            "目标表": str(entry.get("目标载体", "unknown")),
            "文件数量": 85,
            "合并策略": "BinPack-按大小合并",
            "触发阈值": "文件数 >= 50",
            "执行内存": "2GB",
            "合并耗时": "未执行",
            "释放空间": "未执行",
            "阻塞批次": block_batch,
            "status": "合并中",
            "pending": True,
            "abnormal": False,
        }
        comp_rows.append(comp)

        ownership_result: dict[str, Any] | None = None
        if files:
            # 延迟导入避免服务模块间的循环依赖。
            from app.services.shard_maintain import shard_maintain

            registered: list[dict[str, Any]] = []
            for item in files:
                row = shard_maintain.register_ownership({
                    "管道编号": entry.get("管道编号"),
                    "目标表": entry.get("目标载体"),
                    "文件路径": item.get("文件路径"),
                    "文件大小MB": item.get("文件大小MB", 0),
                    "阻塞批次": block_batch,
                    "清理区间": item.get("清理区间"),
                    "共享授权管道": item.get("共享授权管道") or [],
                })
                registered.append(row)
            ownership_result = {
                "阻塞批次": block_batch,
                "登记有效": [r["文件路径"] for r in registered if r["登记状态"] == "登记有效"],
                "越权拦截": [
                    {"文件路径": r["文件路径"], "拒绝原因": r.get("拒绝原因")}
                    for r in registered if r["登记状态"] != "登记有效"
                ],
            }
        comp["归属校验"] = ownership_result
        return comp