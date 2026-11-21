"""数据导出领域服务：交付入口按下载编号呈现快照与实际聚合版本。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataExportService(BaseModuleService):
    MODULE = "data_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "标准化"
    LIST_FIELDS = ["导出范围", "目标格式", "过滤条件", "导出状态", "文件路径"]
    REQUIRED_FIELDS = ["导出范围", "目标格式", "过滤条件"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"开始导出": "执行中", "重试导出": "执行中"}
    NEGATIVE_ACTIONS = []

    # 每次交付都会持久保留的字段，重新进入列表仍可追溯
    EXTRA_FIELDS = [
        "下载编号",
        "view_revision",
        "results_digest",
        "record_count",
        "downloaded_at",
        "reexport_lineage_id",
        "lineage_source",
    ]

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [self._present(row) for row in store.rows(self.MODULE)]
        return self.filter_entries(
            rows, filters=filters, keyword=keyword, status=status, page=page, size=size
        )

    def _present(self, row: dict[str, Any]) -> dict[str, Any]:
        """补全旧导出缺失的下载编号和实际聚合版本，保持新旧结果一致。

        只补齐展示字段，不改动存储里的快照内容，因此已下载文件保持原样。
        """
        presented = {
            key: value for key, value in row.items() if key != "file_content"
        }
        export_id = int(row.get("id", 0))
        presented.setdefault("下载编号", f"EXP-{export_id:04d}")
        presented.setdefault("view_revision", 0)
        presented.setdefault("lineage_source", "export_created")
        return presented
