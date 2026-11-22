"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource
from app.routers import shard_maintain as shard_maintain_router

ROUTERS = [shard_maintain_router, resource]
