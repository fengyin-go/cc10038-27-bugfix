"""聚合查询视图领域服务：列表只呈现每个视图的已确认修订与实际聚合版本。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.pipeline import pipeline
from app.store import store


class AggregationViewService(BaseModuleService):
    MODULE = "aggregation_view"
    ENTITY = "聚合视图"
    OPERATION_TERM = "标准化"
    LIST_FIELDS = ["视图名称", "聚合维度", "指标定义", "数据源范围", "刷新间隔"]
    REQUIRED_FIELDS = ["视图名称", "聚合维度", "指标定义"]
    STATUS_ORDER = ["待生效", "已生效", "已停用"]
    ACTION_RULES = {"启用视图": "已生效", "停用视图": "已停用"}
    NEGATIVE_ACTIONS = ["停用视图"]

    # 列表额外展示的持久字段：每次刷新/交付都保留，重进列表仍能看到
    EXTRA_FIELDS = [
        "实际聚合版本",
        "已确认修订",
        "待确认修订",
        "结果摘要",
        "分组数",
        "刷新确认时间",
        "last_refreshed_at",
        "current_revision",
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
        """同名视图按修订聚合成一行，避免刷新后的旧分组继续出现在列表里。"""
        return self.filter_entries(
            pipeline.list_view_states(),
            filters=filters,
            keyword=keyword,
            status=status,
            page=page,
            size=size,
        )

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """详情与列表同源：都指向视图的已确认修订，而不是某次旧刷新。"""
        state = self._state_for_id(entry_id)
        if state is not None:
            return state
        # 行 id 属于某个历史修订时也回落到同名视图的聚合视图
        raw = super().get_entry(entry_id)
        if raw is None:
            return None
        return next(
            (
                row for row in pipeline.list_view_states()
                if row.get("视图名称") == raw.get("视图名称")
            ),
            raw,
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """启用/停用落在视图定义行上，刷新修订不承担生命周期状态。"""
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        state = self._state_for_id(entry_id)
        view_name = state.get("视图名称") if state is not None else None
        if view_name is None:
            # 旧书签可能指向某个历史修订行，按同名视图归到定义行
            raw = store.find(self.MODULE, entry_id)
            view_name = raw.get("视图名称") if raw is not None else None
        definition = (
            pipeline.view_definition(view_name) if view_name is not None else None
        )
        if definition is not None:
            definition["status"] = target
            definition["pending"] = target != self.STATUS_ORDER[-1]
            definition["abnormal"] = action in self.NEGATIVE_ACTIONS
            definition["manual_stopped"] = target == "已停用"
            refreshed = self._state_for_id(int(definition.get("id", 0)))
            return refreshed if refreshed is not None else state, f"{self.ENTITY}已{action}"

        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        return super().run_action(entry_id, action)

    def _state_for_id(self, entry_id: int) -> dict[str, Any] | None:
        return next(
            (
                row for row in pipeline.list_view_states()
                if int(row.get("id", 0)) == entry_id
            ),
            None,
        )
