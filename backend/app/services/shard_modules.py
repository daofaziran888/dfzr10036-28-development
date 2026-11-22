"""分片维护归属相关表的通用查询服务壳。

领域写操作（授权、登记、申请、领取、替换、对账）在
``app.services.shard_maintain.ShardMaintainService`` 中；这里只提供统一资源路由
所需的列表/详情查询骨架，状态流转不经过通用动作入口。
"""
from __future__ import annotations

from app.services.base import BaseModuleService


class WriteGrantService(BaseModuleService):
    MODULE = "write_grant"
    ENTITY = "写入授权"
    LIST_FIELDS = [
        "授权编号", "管道编号", "目标表", "授权前缀", "授权范围",
        "共享管道", "授权状态", "登记时间",
    ]
    REQUIRED_FIELDS = ["管道编号", "目标表", "授权前缀"]
    STATUS_ORDER = ["生效中", "已撤销"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []


class ShardOwnershipService(BaseModuleService):
    MODULE = "shard_ownership"
    ENTITY = "分片维护归属明细"
    LIST_FIELDS = [
        "归属编号", "目标表", "文件路径", "文件大小MB", "授权管道",
        "授权范围", "共享授权管道", "阻塞批次", "清理区间",
        "登记状态", "替换状态", "锁定批次编号",
    ]
    REQUIRED_FIELDS = ["目标表", "文件路径", "授权管道"]
    STATUS_ORDER = ["登记有效", "授权已撤销", "登记无效-越权"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []


class MaintainBatchService(BaseModuleService):
    MODULE = "maintain_batch"
    ENTITY = "分片维护批次"
    LIST_FIELDS = [
        "批次编号", "目标表", "申请管道", "批次状态",
        "受理路径", "拒绝路径", "已替换路径", "关联合并任务", "申请时间",
    ]
    REQUIRED_FIELDS = ["目标表", "申请管道"]
    STATUS_ORDER = ["已申请", "领取中", "已完成"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []
