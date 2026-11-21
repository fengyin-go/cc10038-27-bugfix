"""验收脚本：INFO 视图 → 交付 → 补 WARN 刷新期间/结束后各申请一次，核对分组与血缘。

步骤与题面一致：
1. 先生成只有 INFO 的视图并拿一份文件；
2. 补进 WARN 后重跑；
3. 重跑期间（未确认修订）与结束后各发一次申请；
4. 核对分组、实际聚合版本和血缘指向。

用法：先在 backend 目录起服务（./run.sh），再执行
    .venv/bin/python -I tests/acceptance_aggregation_revision.py
脚本只依赖标准库，按部署环境指针访问运行中的服务。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import traceback
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("ACCEPTANCE_BASE", "http://127.0.0.1:8013")


def call(method: str, path: str, payload: dict | None = None) -> dict:
    """按服务环境指针发请求；非 JSON 响应（如 CSV 文件）保留原文。"""
    url = BASE + path
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            body = response.read()
            text = body.decode(errors="replace")
            content_type = response.headers.get("Content-Type", "")
            parsed: dict = {}
            if "json" in content_type and text:
                parsed = json.loads(text)
            return {
                "json": parsed,
                "headers": dict(response.headers),
                "text": text,
                "status": response.status,
            }
    except urllib.error.HTTPError as error:
        return {
            "json": {},
            "headers": dict(error.headers),
            "text": error.read().decode(errors="replace"),
            "status": error.code,
        }


def main() -> int:
    checks: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append((name, ok, detail))

    health = call("GET", "/api/health")
    if health["status"] != 200:
        print(f"服务未就绪：{BASE} -> {health['status']} {health['text']}")
        return 2

    # 0. 准备数据源、解析规则与 schema
    call("POST", "/api/pipeline/source/register", {"source_id": "accept-src"})
    call("POST", "/api/pipeline/source/validate?source_id=accept-src")

    batch = call("POST", "/api/pipeline/batch/start", {
        "source_id": "accept-src",
        "time_window": "2026-11-21",
        "raw_logs": [
            {"body": "t1|INFO|a"},
            {"body": "t2|INFO|b"},
        ],
    })["json"]
    batch_no = batch["batch"]["批次编号"]
    call("POST", "/api/pipeline/batch/parse", {"batch_no": batch_no})
    call("POST", f"/api/pipeline/validate?batch_no={batch_no}")

    # 1. 只有 INFO 的视图
    refresh = call("POST", "/api/pipeline/aggregation/refresh", {
        "view_name": "accept-view", "dim_field": "level",
        "metric_field": "time", "agg_func": "count",
    })["json"]
    check("首版视图只有 INFO",
          set(refresh["view"]["results"]) == {"INFO"},
          json.dumps(refresh["view"]["results"], ensure_ascii=False))

    # 2. 拿第一份文件（创建 + 下载）
    old_export = call("POST", "/api/pipeline/export/build", {
        "view_name": "accept-view",
    })["json"]["export"]
    old_download = call(
        "POST", f"/api/pipeline/export/{old_export['id']}/download"
    )["json"]
    old_file = call("GET", f"/api/pipeline/export/{old_export['id']}/file")
    check("旧交付快照只有 INFO", set(old_export["results"]) == {"INFO"})
    check("下载回写下载类血缘",
          old_download["lineage"]["lineage_kind"] == "download"
          and old_download["lineage"]["lineage_source"] == "downloaded_revision",
          json.dumps(old_download["lineage"], ensure_ascii=False, default=str))
    check("文件头带回下载编号与修订",
          old_file["headers"].get("x-download-no") == old_export["下载编号"]
          and old_file["headers"].get("x-view-revision") == "1",
          json.dumps(old_file["headers"], ensure_ascii=False))
    check("下载文件内容不含 WARN", "WARN" not in old_file["text"], old_file["text"])

    # 3. 补进 WARN，重跑（先写入待确认修订）
    batch2 = call("POST", "/api/pipeline/batch/start", {
        "source_id": "accept-src",
        "time_window": "2026-11-21-late",
        "raw_logs": [{"body": "t3|WARN|c"}],
    })["json"]
    batch_no2 = batch2["batch"]["批次编号"]
    call("POST", "/api/pipeline/batch/parse", {"batch_no": batch_no2})
    call("POST", f"/api/pipeline/validate?batch_no={batch_no2}")

    pending = call("POST", "/api/pipeline/aggregation/refresh", {
        "view_name": "accept-view", "dim_field": "level",
        "metric_field": "time", "agg_func": "count", "confirm": False,
    })["json"]
    check("重跑先写入待确认修订", pending["status"] == "待生效",
          json.dumps(pending, ensure_ascii=False))

    # 4a. 重跑期间发一次申请：应只拿到已确认的旧修订
    during_export = call("POST", "/api/pipeline/export/build", {
        "view_name": "accept-view",
    })["json"]["export"]
    check("重跑期间申请仍读到旧修订",
          during_export["view_revision"] == 1
          and set(during_export["results"]) == {"INFO"},
          json.dumps(during_export["results"], ensure_ascii=False))

    during_open = call(
        "GET", f"/api/pipeline/export/{old_export['id']}/open"
    )["json"]
    check("重跑期间重开旧下载内容不变",
          during_open["results"] == old_export["results"]
          and during_open["view_revision"] == 1)

    states = call("GET", "/api/pipeline/aggregation/views")["json"]["items"]
    view_state = next(item for item in states if item["视图名称"] == "accept-view")
    check("列表显示实际聚合版本与待确认修订",
          view_state["实际聚合版本"] == 1 and view_state["待确认修订"] == 2,
          json.dumps(view_state, ensure_ascii=False, default=str))

    # 5. 确认刷新后发一次申请：应读到含 WARN 的新修订
    confirm = call("POST", "/api/pipeline/aggregation/confirm", {
        "view_name": "accept-view",
    })["json"]
    check("刷新确认成功", confirm["ok"] is True, confirm["message"])

    after_export = call("POST", "/api/pipeline/export/build", {
        "view_name": "accept-view",
    })["json"]["export"]
    check("确认后申请读到新修订并含 WARN",
          after_export["view_revision"] == 2
          and set(after_export["results"]) == {"INFO", "WARN"}
          and after_export["results"]["WARN"]["count"] == 1,
          json.dumps(after_export["results"], ensure_ascii=False))

    # 6. 已下载旧文件保持原内容，血缘可按下载编号区分
    reopened = call("GET", f"/api/pipeline/export/{old_export['id']}/open")["json"]
    check("确认后旧下载内容仍保持原样",
          reopened["results"] == old_export["results"]
          and reopened["results_digest"] == old_export["results_digest"])

    lineage = call("GET", "/api/data_lineage?size=200")["json"]["items"]
    old_lines = [
        line for line in lineage
        if str(line.get("export_id")) == str(old_export["id"])
    ]
    kinds = {line.get("lineage_kind") for line in old_lines}
    check("同一交付同时保留创建与下载两类血缘",
          kinds == {"export_created", "download"},
          json.dumps(old_lines, ensure_ascii=False, default=str))
    check("血缘指向旧修订而非最新修订",
          all(line.get("view_revision") == 1 for line in old_lines),
          json.dumps(old_lines, ensure_ascii=False, default=str))

    # 7. 并发：交付与聚合同时发起
    barrier = threading.Barrier(2)
    results: dict[str, object] = {}

    def do_export() -> None:
        barrier.wait()
        results["export"] = call("POST", "/api/pipeline/export/build", {
            "view_name": "accept-view",
        })["json"]

    def do_refresh() -> None:
        barrier.wait()
        results["refresh"] = call("POST", "/api/pipeline/aggregation/refresh", {
            "view_name": "accept-view", "dim_field": "level",
            "metric_field": "time", "agg_func": "count",
        })["json"]

    threads = [threading.Thread(target=do_export), threading.Thread(target=do_refresh)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    export_result = results.get("export") or {}
    refresh_result = results.get("refresh") or {}
    check("并发的交付与聚合都返回可核对边界",
          export_result.get("ok") is True and refresh_result.get("ok") is True,
          json.dumps(results, ensure_ascii=False, default=str))
    if export_result.get("ok"):
        delivered = export_result["export"]
        check("并发交付的结果摘要与修订可核对",
              delivered["view_revision"] in (2, 3)
              and bool(delivered["results_digest"]),
              json.dumps(delivered["results"], ensure_ascii=False))

    revisions = call(
        "GET", "/api/pipeline/aggregation/views/accept-view/revisions"
    )["json"]
    active = [item for item in revisions["revisions"] if item["status"] == "已生效"]
    check("视图修订清单只保留一个当前修订",
          revisions["current_revision"] is not None and len(active) == 1,
          json.dumps(revisions, ensure_ascii=False, default=str))

    failed = [item for item in checks if not item[1]]
    for name, ok, detail in checks:
        print(("PASS " if ok else "FAIL ") + name + ("" if ok else f" -> {detail}"))
    print(f"\n{len(checks) - len(failed)}/{len(checks)} 项通过")
    return 0 if not failed else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(2)
