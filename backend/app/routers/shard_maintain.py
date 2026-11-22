"""分片维护归属专用端点。

通用列表/详情仍走 ``/api/{module}`` 资源路由（write_grant / shard_ownership /
maintain_batch 三张表都已注册服务）；本路由承载带领域判断的写操作与对账回传。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.shard_maintain import shard_maintain

router = APIRouter(prefix="/api/shard-maintain", tags=["分片维护归属"])


class GrantPayload(BaseModel):
    管道编号: str
    目标表: str
    授权前缀: str
    授权范围: str = "专有"
    共享管道: list[str] = Field(default_factory=list)


class RevokePayload(BaseModel):
    管道编号: str
    目标表: str
    授权前缀: str | None = None


class OwnershipPayload(BaseModel):
    管道编号: str
    目标表: str
    文件路径: str
    文件大小MB: Any = 0
    阻塞批次: str | None = None
    清理区间: str | None = None
    共享授权管道: list[str] = Field(default_factory=list)


class ApplyPayload(BaseModel):
    管道编号: str
    目标表: str
    路径清单: list[str]


class ReplacePayload(BaseModel):
    路径清单: list[str]
    替换证据: str | None = None


def _guard(exc: ValueError) -> dict[str, Any]:
    return {"ok": False, "message": str(exc)}


@router.post("/grants")
def register_grant(payload: GrantPayload) -> dict[str, Any]:
    try:
        grant = shard_maintain.register_grant(payload.model_dump())
    except ValueError as exc:
        return _guard(exc)
    return {"ok": True, "entry": grant}


@router.post("/grants/revoke")
def revoke_grant(payload: RevokePayload) -> dict[str, Any]:
    return shard_maintain.revoke_grant(payload.管道编号, payload.目标表, prefix=payload.授权前缀)


@router.post("/ownership")
def register_ownership(payload: OwnershipPayload) -> dict[str, Any]:
    try:
        row = shard_maintain.register_ownership(payload.model_dump())
    except ValueError as exc:
        return _guard(exc)
    return {
        "ok": row["登记状态"] == "登记有效",
        "entry": row,
        "message": "归属登记有效" if row["登记状态"] == "登记有效"
        else f"归属登记被拒绝：{row.get('拒绝原因')}",
    }


@router.get("/ownership/detail")
def ownership_detail(目标表: str | None = None, 文件路径: str | None = None) -> dict[str, Any]:
    items = shard_maintain.ownership_detail(table=目标表, path=文件路径)
    return {"items": items, "total": len(items)}


@router.post("/batches")
def apply_maintenance(payload: ApplyPayload) -> dict[str, Any]:
    try:
        return shard_maintain.apply_maintenance(payload.model_dump())
    except ValueError as exc:
        return _guard(exc)


@router.post("/batches/{batch_id}/claim")
def claim_objects(batch_id: int) -> dict[str, Any]:
    return shard_maintain.claim_objects(batch_id)


@router.post("/batches/{batch_id}/replace")
def confirm_replacement(batch_id: int, payload: ReplacePayload) -> dict[str, Any]:
    return shard_maintain.confirm_replacement(
        batch_id, payload.路径清单, replacement=payload.替换证据)


@router.get("/batches/{batch_id}/reconcile")
def reconcile(batch_id: int) -> dict[str, Any]:
    return shard_maintain.reconcile(batch_id)


@router.get("/report/{table}")
def source_report(table: str) -> dict[str, Any]:
    return shard_maintain.source_report(table)
