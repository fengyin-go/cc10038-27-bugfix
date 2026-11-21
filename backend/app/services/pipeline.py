"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import threading
import time
from typing import Any

from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _download_no(export_id: int) -> str:
    """下载编号：保留数字主体，便于兼容早期只按 id 使用的下载条目。"""
    return f"EXP-{int(export_id):04d}"


def _results_csv(results: dict[str, Any]) -> str:
    """把聚合分组渲染成可下载文件；按维度排序，保证同一修订内容稳定。"""
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["dimension", "count", "sum"])
    for dimension in sorted(results):
        entry = results.get(dimension) or {}
        writer.writerow([dimension, entry.get("count", 0), entry.get("sum", 0)])
    return output.getvalue()


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。

    聚合视图按“视图名称”维护一条记录和多个修订：刷新先暂存待确认修订，
    确认后才替换生效修订。交付入口只读取生效修订的深拷贝快照，因此刷新
    写入期间和写入之后，已经下载的文件都不会被新分组污染。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
        confirm: bool = True,
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图，生成一个新修订。

        刷新永远追加修订，不会原地覆盖生效修订：`confirm=False` 只写入
        待确认修订（`status=待生效`），供与交付并发时核对边界；确认后才
        把新修订提升为生效修订。按视图名称查找时只认已确认修订。
        """
        parsed = list(store.rows("parsed_entity"))
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        refined_results = copy.deepcopy(results)

        with self._lock:
            aggs = store.rows("aggregation_view")
            view_entry, revisions = self._resolve_view(aggs, view_name)
            same_name = list(revisions)
            if view_entry is None or isinstance(view_entry.get("results"), dict):
                view_entry = self._ensure_view_definition(aggs, view_name, revisions)
                revisions = [r for r in aggs if r.get("视图名称") == view_name]
                same_name = list(revisions)

            # 修订号在视图内单调递增，不使用行 id，避免同视图多修订互相覆盖
            revision = max(
                (int(r.get("revision", 0)) for r in same_name), default=0
            ) + 1

            agg_entry = {
                "id": _next_id("aggregation_view"),
                "视图名称": view_name,
                "聚合维度": dim_field,
                "指标定义": f"{agg_func}({metric_field})",
                "数据源范围": "标准化明细",
                "刷新间隔": "manual",
                "status": "待生效",
                "pending": True,
                "abnormal": False,
                "results": refined_results,
                "results_digest": _stable_digest(refined_results),
                "refreshed_at": _now_iso(),
                "source_records": len(parsed),
                "revision": revision,
                "confirmed": False,
            }
            aggs.append(agg_entry)

            if confirm:
                # 只把修订行转为历史，视图定义行的生命周期状态由动作维护
                self._promote_revision(
                    view_entry,
                    [r for r in revisions if r.get("视图名称") == view_name],
                    agg_entry,
                )
            elif view_entry is not None:
                # 待确认修订挂在视图元数据行上，交付读取时不会选中它
                view_entry["pending_revision"] = revision

            status = agg_entry["status"]

        return {
            "ok": True,
            "message": (
                f"聚合视图 {view_name} 修订 {revision} 已刷新"
                + ("" if confirm else "（待确认）")
            ),
            "view": agg_entry,
            "revision": revision,
            "status": status,
            "confirmed": confirm,
            "dimensions": len(refined_results),
        }

    def confirm_aggregation(
        self, view_name: str, revision: int | None = None
    ) -> dict[str, Any]:
        """确认待确认修订：确认后它成为交付入口唯一可读取的版本。"""
        with self._lock:
            aggs = store.rows("aggregation_view")
            view_entry, revisions = self._resolve_view(aggs, view_name)
            if not revisions:
                return {"ok": False, "message": f"聚合视图 {view_name} 不存在"}

            pending = [r for r in revisions if r.get("status") == "待生效"]
            if revision is not None:
                pending = [
                    r for r in pending if int(r.get("revision", 0)) == int(revision)
                ]
            if not pending:
                return {
                    "ok": False,
                    "message": f"聚合视图 {view_name} 没有待确认修订",
                }

            target = pending[-1]
            self._promote_revision(
                view_entry,
                [r for r in revisions if r is not target],
                target,
            )
            return {
                "ok": True,
                "message": (
                    f"聚合视图 {view_name} 修订 {target['revision']} 已确认"
                ),
                "view": target,
                "revision": int(target["revision"]),
                "digest": target.get("results_digest", ""),
            }

    @staticmethod
    def _promote_revision(
        view_entry: dict[str, Any] | None,
        others: list[dict[str, Any]],
        target: dict[str, Any],
    ) -> None:
        """把目标修订提升为生效版本，其余同名修订转为历史，一次完成。"""
        for old in others:
            if old is target:
                continue
            if isinstance(old.get("results"), dict) and old.get("status") != "已停用":
                old["status"] = "已停用"
                old["pending"] = False
                old["superseded_by"] = str(target.get("revision", ""))

        target["status"] = "已生效"
        target["pending"] = False
        target["confirmed"] = True
        no_definition = view_entry is None or isinstance(
            view_entry.get("results"), dict
        )
        if no_definition:
            target["current_revision"] = int(target.get("revision", 0))
            target["current_digest"] = target.get("results_digest", "")
            target["last_refreshed_at"] = target.get("refreshed_at", _now_iso())
            return
        view_entry["current_revision"] = int(target.get("revision", 0))
        view_entry["current_digest"] = target.get("results_digest", "")
        view_entry["last_refreshed_at"] = target.get("refreshed_at", _now_iso())
        view_entry.pop("pending_revision", None)
        # 确认刷新让视图进入生效态；但管理员显式停用的视图保持停用，
        # 刷新只推进修订指针，不覆盖生命周期状态
        if not view_entry.get("manual_stopped"):
            view_entry["status"] = "已生效"
            view_entry["pending"] = False

    # ---------- 聚合视图版本解析 ----------
    @staticmethod
    def _resolve_revisions(
        rows: list[dict[str, Any]], view_name: str
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """整理同名视图的修订行，兼容旧数据里散落的同名行。

        只有带 `results` 的行才是刷新产物；仅登记、从未刷新过的行只作为
        视图定义。没有修订号的历史刷新行按 id 顺序补齐修订号和摘要。
        同名视图只允许一个已确认修订，多余的“已生效”行视为旧结果残留
        转为历史，避免同名视图出现两版。
        """
        same_name = [r for r in rows if r.get("视图名称") == view_name]
        if not same_name:
            return None, []

        refreshed = [r for r in same_name if isinstance(r.get("results"), dict)]
        legacy = sorted(
            (r for r in refreshed if "revision" not in r),
            key=lambda r: int(r.get("id", 0)),
        )
        for index, row in enumerate(legacy, start=1):
            row["revision"] = index
            row.setdefault("confirmed", True)
            if "results_digest" not in row:
                row["results_digest"] = _stable_digest(row.get("results") or {})

        ordered = sorted(
            refreshed, key=lambda r: (int(r.get("revision", 0)), int(r.get("id", 0)))
        )
        definitions = sorted(
            (r for r in same_name if not isinstance(r.get("results"), dict)),
            key=lambda r: int(r.get("id", 0)),
        )
        view_entry = next(
            (r for r in same_name if "current_revision" in r), None
        )
        if view_entry is None and definitions:
            view_entry = definitions[0]

        active = [
            r for r in ordered
            if r.get("status") == "已生效" and r.get("confirmed") is not False
        ]
        if len(active) > 1:
            keep = active[-1]
            for row in active[:-1]:
                row["status"] = "已停用"
                row["pending"] = False
                row["superseded_by"] = str(keep.get("revision", ""))
            active = [keep]

        if active and view_entry is not None:
            if "current_revision" not in view_entry:
                view_entry["current_revision"] = int(active[-1].get("revision", 0))
            if "current_digest" not in view_entry:
                view_entry["current_digest"] = active[-1].get("results_digest", "")
        return view_entry, ordered

    def _resolve_view(
        self, rows: list[dict[str, Any]], view_name: str
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        return self._resolve_revisions(rows, view_name)

    @staticmethod
    def _ensure_view_definition(
        rows: list[dict[str, Any]],
        view_name: str,
        revisions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """旧数据只有刷新行时补一条视图定义行，避免列表与动作没有稳定落点。"""
        existing = next(
            (
                r for r in rows
                if r.get("视图名称") == view_name
                and not isinstance(r.get("results"), dict)
            ),
            None,
        )
        if existing is not None:
            return existing
        latest = revisions[-1] if revisions else {}
        definition = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": latest.get("聚合维度", ""),
            "指标定义": latest.get("指标定义", ""),
            "数据源范围": latest.get("数据源范围", "标准化明细"),
            "刷新间隔": latest.get("刷新间隔", "manual"),
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "current_revision": int(latest.get("revision", 0)),
            "current_digest": latest.get("results_digest", ""),
            "last_refreshed_at": latest.get("refreshed_at", ""),
        }
        rows.append(definition)
        return definition

    def view_definition(self, view_name: str) -> dict[str, Any] | None:
        """返回承载生命周期状态（启用/停用）的视图定义行。"""
        with self._lock:
            rows = store.rows("aggregation_view")
            view_entry, revisions = self._resolve_view(rows, view_name)
            if not revisions and view_entry is None:
                return None
            if view_entry is not None and not isinstance(
                view_entry.get("results"), dict
            ):
                return view_entry
            return self._ensure_view_definition(rows, view_name, revisions)

    # ---------- 聚合视图状态读取 ----------
    def list_view_states(self) -> list[dict[str, Any]]:
        """每个视图只输出一行：生效修订字段 + 实际修订与待确认边界。"""
        with self._lock:
            rows = store.rows("aggregation_view")
            names: list[str] = []
            for row in rows:
                name = str(row.get("视图名称") or "")
                if name and name not in names:
                    names.append(name)

            states: list[dict[str, Any]] = []
            for name in names:
                view_entry, revisions = self._resolve_view(rows, name)
                if view_entry is None or isinstance(view_entry.get("results"), dict):
                    view_entry = self._ensure_view_definition(rows, name, revisions)
                confirmed = [
                    r for r in revisions
                    if r.get("status") == "已生效" and r.get("confirmed") is not False
                ]
                pending = [r for r in revisions if r.get("status") == "待生效"]
                current = confirmed[-1] if confirmed else None
                # 行以视图定义行为身份，刷新只改变展示的修订信息，
                # 这样重新进入列表时 id 与字段保持稳定
                state = {
                    key: value
                    for key, value in view_entry.items()
                    if key not in ("results", "file_content", "revision", "confirmed")
                }
                # 定义行自身记录了管理员显式的启用/停用，刷新确认不覆盖停用
                if current is not None and state.get("status") != "已停用":
                    state["status"] = "已生效"
                    state["pending"] = False
                elif current is None and pending:
                    state["status"] = "待生效"
                    state["pending"] = True
                state.update({
                    "视图名称": name,
                    "实际聚合版本": (
                        int(current.get("revision", 0)) if current else "未刷新"
                    ),
                    "已确认修订": (
                        int(current.get("revision", 0)) if current else ""
                    ),
                    "待确认修订": (
                        int(pending[-1].get("revision", 0)) if pending else ""
                    ),
                    "结果摘要": (
                        current.get("results_digest", "") if current else ""
                    ),
                    "分组数": len(current.get("results") or {}) if current else 0,
                    "刷新确认时间": (
                        current.get("refreshed_at", "") if current else ""
                    ),
                    "current_revision": view_entry.get("current_revision"),
                    "pending_revision": view_entry.get("pending_revision", ""),
                })
                states.append(state)
            return states

    def get_view_revisions(self, view_name: str) -> dict[str, Any]:
        """列出视图全部修订及其确认状态，供并发时核对边界。"""
        with self._lock:
            rows = store.rows("aggregation_view")
            view_entry, revisions = self._resolve_view(rows, view_name)
            if not revisions:
                return {"ok": False, "message": f"聚合视图 {view_name} 不存在"}

            current = next(
                (
                    r for r in reversed(revisions)
                    if r.get("status") == "已生效" and r.get("confirmed") is not False
                ),
                None,
            )
            pending = next(
                (r for r in reversed(revisions) if r.get("status") == "待生效"),
                None,
            )
            return {
                "ok": True,
                "view_name": view_name,
                "current_revision": (
                    int(current.get("revision", 0)) if current else None
                ),
                "current_digest": (
                    current.get("results_digest", "") if current else ""
                ),
                "pending_revision": (
                    int(pending.get("revision", 0)) if pending else None
                ),
                "pending_digest": (
                    pending.get("results_digest", "") if pending else ""
                ),
                "revisions": [
                    {
                        "revision": int(r.get("revision", 0)),
                        "status": r.get("status"),
                        "confirmed": r.get("confirmed", True) is not False,
                        "dimensions": len(r.get("results") or {}),
                        "results_digest": r.get("results_digest", ""),
                        "refreshed_at": r.get("refreshed_at", ""),
                        "superseded_by": r.get("superseded_by", ""),
                    }
                    for r in revisions
                ],
            }

    def _confirmed_revision(
        self, view_name: str, revision: int | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        """只从已确认（status=已生效）修订取数；待确认修订不可被交付读取。"""
        with self._lock:
            rows = store.rows("aggregation_view")
            view_entry, revisions = self._resolve_view(rows, view_name)
            if not revisions:
                return None, f"聚合视图 {view_name} 不存在"
            confirmed = [
                r for r in revisions
                if r.get("status") == "已生效" and r.get("confirmed") is not False
            ]
            if revision is not None:
                confirmed = [
                    r for r in confirmed if int(r.get("revision", 0)) == revision
                ]
                if not confirmed:
                    return None, f"聚合视图 {view_name} 的修订 {revision} 尚未确认"
            else:
                # 没有显式修订时，取视图登记的当前生效修订
                current = (
                    view_entry.get("current_revision") if view_entry is not None else None
                )
                matched = [
                    r for r in confirmed
                    if current is not None and int(r.get("revision", 0)) == int(current)
                ]
                if matched:
                    confirmed = matched
                elif len(confirmed) > 1:
                    confirmed = sorted(
                        confirmed, key=lambda r: int(r.get("revision", 0))
                    )[-1:]
            if not confirmed:
                return None, f"聚合视图 {view_name} 没有已确认的可用修订"
            return confirmed[-1], ""

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
        view_revision: int | None = None,
    ) -> dict[str, Any]:
        """从聚合视图的已确认修订生成可下载的日志聚合修订。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        with self._lock:
            view, error = self._confirmed_revision(view_name, view_revision)
            if view is None:
                return {"ok": False, "message": error}

            # 交付入口拿到的是深拷贝快照：后续刷新写入不会改动这份结果
            results_data = copy.deepcopy(view.get("results") or {})
            revision = int(view.get("revision", 0))
            digest = str(view.get("results_digest") or _stable_digest(results_data))

            export_id = _next_id("data_export")
            download_no = _download_no(export_id)
            export_entry = {
                "id": export_id,
                "下载编号": download_no,
                "导出范围": view_name,
                "目标格式": export_format,
                "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
                "导出状态": "已生成",
                "文件路径": f"/exports/{view_name}-r{revision}.{export_format}",
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "results": results_data,
                "results_digest": digest,
                "view_revision": revision,
                "view_revision_status": "已生效",
                "view_name": view_name,
                "record_count": len(results_data),
                "file_content": _results_csv(results_data),
                "downloaded_at": "",
                "reexport_lineage_id": "",
                "lineage_source": "confirmed_revision",
                "exported_at": _now_iso(),
            }
            exports.append(export_entry)

            # 记录血缘：下载尚未发生，血缘先登记“创建”依据
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": view_name,
                "解析规则版本": f"{view_name}@r{revision}",
                "标准化版本": f"{view_name}@r{revision}",
                "下游消费标识": f"export-{export_id}",
                "记录时间": _now_iso(),
                "status": "已记录",
                "pending": False,
                "abnormal": False,
                "export_id": str(export_id),
                "download_no": download_no,
                "export_format": export_format,
                "view_revision": revision,
                "results_digest": digest,
                "lineage_kind": "export_created",
                "lineage_source": "confirmed_revision",
            }
            lineages.append(lineage_entry)
            export_entry["lineage_id"] = str(lineage_entry["id"])

        return {
            "ok": True,
            "message": f"数据导出 {view_name} r{revision}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 已下载修订回写血缘 ----------
    def download_export(self, export_id: int) -> dict[str, Any]:
        """按下载编号/id 读取已生成的导出；每次下载回写一条下载血缘。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        with self._lock:
            export_entry = store.find("data_export", export_id)
            if export_entry is None:
                return {"ok": False, "message": f"导出任务 {export_id} 不存在"}
            download_no = str(
                export_entry.get("下载编号") or _download_no(export_entry["id"])
            )
            export_entry["下载编号"] = download_no

            # 旧导出若无快照，退回其登记时的结果，保持内容不变
            snapshot = copy.deepcopy(export_entry.get("results") or {})
            digest = str(
                export_entry.get("results_digest") or _stable_digest(snapshot)
            )
            export_entry["results_digest"] = digest

            downloaded_at = _now_iso()
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": str(export_entry.get("导出范围") or ""),
                "解析规则版本": f"{export_entry.get('导出范围')}@r{export_entry.get('view_revision')}",
                "标准化版本": f"{export_entry.get('导出范围')}@r{export_entry.get('view_revision')}",
                "下游消费标识": f"download-{download_no}",
                "记录时间": downloaded_at,
                "status": "已记录",
                "pending": False,
                "abnormal": False,
                "export_id": str(export_entry["id"]),
                "download_no": download_no,
                "export_format": export_entry.get("目标格式", ""),
                "view_revision": int(export_entry.get("view_revision", 0) or 0),
                "results_digest": digest,
                "lineage_kind": "download",
                "lineage_source": "downloaded_revision",
                "download_round": sum(
                    1 for line in lineages
                    if line.get("lineage_kind") == "download"
                    and str(line.get("export_id")) == str(export_entry["id"])
                ) + 1,
            }
            lineages.append(lineage_entry)

            export_entry["downloaded_at"] = downloaded_at
            export_entry["reexport_lineage_id"] = str(lineage_entry["id"])
            export_entry["lineage_source"] = "downloaded_revision"
            export_entry["download_count"] = int(
                export_entry.get("download_count", 0) or 0
            ) + 1

            return {
                "ok": True,
                "message": f"{download_no} 已下载，血缘 {lineage_entry['id']} 已回写",
                "export": export_entry,
                "lineage": lineage_entry,
                "download_no": download_no,
                "view_revision": lineage_entry["view_revision"],
                "results": snapshot,
            }

    def open_download(self, export_id: int) -> dict[str, Any]:
        """重新打开同一次下载：只复用当时快照，不重新取最新修订。"""
        with self._lock:
            export_entry = store.find("data_export", export_id)
            if export_entry is None:
                return {"ok": False, "message": f"导出任务 {export_id} 不存在"}
            download_no = str(
                export_entry.get("下载编号") or _download_no(export_entry["id"])
            )
            snapshot = copy.deepcopy(export_entry.get("results") or {})
            return {
                "ok": True,
                "message": f"{download_no} 已按原快照打开",
                "download_no": download_no,
                "view_revision": int(export_entry.get("view_revision", 0) or 0),
                "results_digest": str(
                    export_entry.get("results_digest") or _stable_digest(snapshot)
                ),
                "results": snapshot,
                "lineage_source": export_entry.get("lineage_source", "export_created"),
            }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()