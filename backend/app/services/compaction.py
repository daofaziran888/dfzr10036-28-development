"""小文件合并服务：合并后级联Iceberg过期清理和Metric聚合更新。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "合并"
    LIST_FIELDS = ["任务编号", "目标表", "文件数量", "合并策略", "触发阈值", "执行内存", "合并耗时", "释放空间"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "文件数量"]
    STATUS_ORDER = ["待执行", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "合并中", "取消合并": "已完成", "标记失败": "合并失败"}
    NEGATIVE_ACTIONS = []

    def claim_by_ownership(self, entry_id: int, batch_id: int) -> dict[str, Any] | None:
        """合并任务依据分片维护归属明细，从指定维护批次领取可操作对象。

        领取结果（含仍阻塞路径）回写到合并任务，供维护侧核对。
        """
        from app.services.shard_maintain import shard_maintain

        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        result = shard_maintain.claim_objects(batch_id)
        if not result.get("ok"):
            return result
        entry["领取批次编号"] = result["批次编号"]
        entry["可操作对象"] = result["可操作对象"]
        entry["仍阻塞路径"] = result["仍阻塞路径"]
        entry["claim_token"] = result["claim_token"]
        return result

    def report_by_source(self, entry_id: int) -> dict[str, Any] | None:
        """回传每个来源（管道）实际释放的空间与仍阻塞路径。"""
        from app.services.shard_maintain import shard_maintain

        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        report = shard_maintain.source_report(str(entry.get("目标表", "")))
        entry["来源释放空间MB"] = report["按来源释放空间MB"]
        entry["仍阻塞路径"] = report["仍阻塞路径"]
        return report

    def trigger_expire_and_metric(self, entry_id: int) -> dict[str, Any] | None:
        """合并完成后触发expire_snap清理和metric_aggr聚合更新。

        数据流链路: compaction -> expire_snap + metric_aggr
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已完成":
            return None

        table = str(entry.get("目标表", "unknown"))
        results: dict[str, Any] = {"expire_snap": None, "metric_aggr": None}

        # 触发 expire_snap
        for exp in store.rows("expire_snap"):
            if str(exp.get("目标表", "")) == table:
                exp["status"] = "预演完成"
                exp["pending"] = True
                exp["abnormal"] = False
                exp["预演结果"] = f"可清理{entry.get('文件数量', 0) * 3}个历史快照"
                results["expire_snap"] = exp
                break

        # 创建 metric_aggr 聚合
        metric_rows = store.rows("metric_aggr")
        new_metric_id = max((int(r.get("id", 0)) for r in metric_rows), default=0) + 1
        metric = {
            "id": new_metric_id,
            "窗口编号": f"METR-{new_metric_id:04d}",
            "数据流": table,
            "聚合键": "合并批次",
            "聚合函数": "SUM(file_count), AVG(file_size)",
            "窗口宽度": "1h",
            "输出频率": "每次合并后",
            "延迟更新": "实时",
            "最后写入": "2026-11-18 10:42:00",
            "status": "聚合中",
            "pending": True,
            "abnormal": False,
        }
        metric_rows.append(metric)
        results["metric_aggr"] = metric

        return results