"""流水线路由：暴露跨模块数据采集流水线的 HTTP 接口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.pipeline import pipeline

router = APIRouter(prefix="/api/pipeline", tags=["采集流水线"])


class SourceRegisterPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    protocol: str = "http"
    region: str = "default"


class IngestBatchPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)


class ParseBatchPayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    parser_rule_name: str | None = None


class DedupPayload(BaseModel):
    dedup_key_fields: list[str] = Field(..., min_length=1)
    window: str = "batch"
    retain_strategy: str = "keep_first"


class AggregationPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    dim_field: str = Field(..., min_length=1)
    metric_field: str = "count"
    agg_func: str = "count"
    confirm: bool = True


class AggregationConfirmPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    revision: int | None = None


class ExportPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    export_format: str = "csv"
    filters: dict[str, str] | None = None
    view_revision: int | None = None


class WatermarkPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    batch_no: str = Field(..., min_length=1)
    watermark_value: str = Field(..., min_length=1)


class ReplayPayload(BaseModel):
    history_batch_range: str = Field(..., min_length=1)
    new_parser_rule: str = Field(..., min_length=1)


class SchemaEvolvePayload(BaseModel):
    version: str = Field(..., min_length=1)
    data_source: str = Field(..., min_length=1)
    field_defs: list[str] = Field(..., min_length=1)
    backward_compatible: bool = True


class FullPipelinePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)
    parser_rule: str | None = None


@router.post("/source/register")
def pipeline_register_source(payload: SourceRegisterPayload) -> dict[str, Any]:
    return pipeline.register_source(payload.source_id, payload.protocol, payload.region)


@router.post("/source/validate")
def pipeline_validate_source(source_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.validate_source(source_id)


@router.post("/batch/start")
def pipeline_start_batch(payload: IngestBatchPayload) -> dict[str, Any]:
    return pipeline.start_ingest_batch(payload.source_id, payload.time_window, payload.raw_logs)


@router.post("/batch/parse")
def pipeline_parse_batch(payload: ParseBatchPayload) -> dict[str, Any]:
    return pipeline.parse_batch(payload.batch_no, payload.parser_rule_name)


@router.post("/batch/recover")
def pipeline_recover_batch(batch_no: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.recover_batch(batch_no)


@router.post("/dedup/run")
def pipeline_run_dedup(payload: DedupPayload) -> dict[str, Any]:
    return pipeline.run_dedup(payload.dedup_key_fields, payload.window, payload.retain_strategy)


@router.post("/validate")
def pipeline_validate_entities(batch_no: str | None = Query(default=None)) -> dict[str, Any]:
    return pipeline.validate_entities(batch_no)


@router.post("/aggregation/refresh")
def pipeline_refresh_aggregation(payload: AggregationPayload) -> dict[str, Any]:
    return pipeline.refresh_aggregation(
        payload.view_name,
        payload.dim_field,
        payload.metric_field,
        payload.agg_func,
        payload.confirm,
    )


@router.post("/aggregation/confirm")
def pipeline_confirm_aggregation(payload: AggregationConfirmPayload) -> dict[str, Any]:
    return pipeline.confirm_aggregation(payload.view_name, payload.revision)


@router.get("/aggregation/views")
def pipeline_list_views() -> dict[str, Any]:
    """每个视图一行：已确认修订、待确认修订与实际聚合版本。"""
    states = pipeline.list_view_states()
    return {"items": states, "total": len(states)}


@router.get("/aggregation/views/{view_name}/revisions")
def pipeline_view_revisions(view_name: str) -> dict[str, Any]:
    """核对同一视图各修订的确认状态与结果摘要。"""
    return pipeline.get_view_revisions(view_name)


@router.post("/export/build")
def pipeline_build_export(payload: ExportPayload) -> dict[str, Any]:
    return pipeline.build_export(
        payload.view_name, payload.export_format, payload.filters, payload.view_revision
    )


@router.post("/export/{export_id}/download")
def pipeline_download_export(export_id: int) -> dict[str, Any]:
    """读取可下载的日志聚合修订；下载结果回写血缘清单。"""
    return pipeline.download_export(export_id)


@router.get("/export/{export_id}/open")
def pipeline_open_export(export_id: int) -> dict[str, Any]:
    """重新打开同一次下载：按原快照返回，不跟随最新修订。"""
    return pipeline.open_download(export_id)


@router.get("/export/{export_id}/file")
def pipeline_export_file(export_id: int) -> StreamingResponse:
    """下载可下载的日志聚合修订文件，并回写本次下载的血缘。"""
    result = pipeline.download_export(export_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    filename = f"{result['download_no']}.csv"
    return StreamingResponse(
        iter([result["export"].get("file_content", "")]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Download-No": result["download_no"],
            "X-View-Revision": str(result["view_revision"]),
            "X-Lineage-Id": str(result["lineage"]["id"]),
        },
    )


@router.post("/watermark/advance")
def pipeline_advance_watermark(payload: WatermarkPayload) -> dict[str, Any]:
    return pipeline.advance_watermark(payload.source_id, payload.batch_no, payload.watermark_value)


@router.post("/replay/batch")
def pipeline_replay_batch(payload: ReplayPayload) -> dict[str, Any]:
    return pipeline.replay_batch(payload.history_batch_range, payload.new_parser_rule)


@router.post("/schema/evolve")
def pipeline_evolve_schema(payload: SchemaEvolvePayload) -> dict[str, Any]:
    return pipeline.evolve_schema(
        payload.version, payload.data_source, payload.field_defs, payload.backward_compatible
    )


@router.post("/full")
def pipeline_run_full(payload: FullPipelinePayload) -> dict[str, Any]:
    return pipeline.run_full_pipeline(
        payload.source_id, payload.time_window, payload.raw_logs, payload.parser_rule
    )


@router.get("/snapshot")
def pipeline_get_snapshot() -> dict[str, Any]:
    """获取所有模块的数据快照，用于跨模块数据流验证。"""
    from app.services.base import BaseModuleService
    from app.services import SERVICES
    snapshots: dict[str, Any] = {}
    for key, svc in SERVICES.items():
        snapshots[key] = svc.domain_snapshot()
    return {"modules": snapshots}