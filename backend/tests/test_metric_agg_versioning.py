"""指标聚合口径版本化测试。

覆盖验收场景：
1. 用一条「订单额求和」的新已发布定义触发聚合，结果携带编号/公式/精度快照；
2. 用不存在的编号触发聚合，确认上一条合法结果原样保留、默认指标不会被写入；
3. 更改精度后再次发布，历史口径版本可核对，最新值按新公式/精度重新算得出来；
4. 数据服务读取结果自带口径，而不是用当前定义给旧值重新贴标签；
5. 草稿/未发布/公式不支持等错误路径一律不覆盖既有结果。
"""
from __future__ import annotations

from app.fixtures import TREND_POINTS
from app.services.agg_task import AggTaskService
from app.services.metric_compute import batch_compute_for_agg_task, compute_metric
from app.services.metric_def import MetricDefService
from app.services.pipeline_orchestrator import (
    get_metric_agg_record,
    list_pipeline_results,
    run_all_pipelines,
    run_metric_agg_pipeline,
)
from app.store import store

GMV_TOTAL = sum(p["gmv"] for p in TREND_POINTS)
METRIC_PIPELINE_ID = "metric_agg_api"


def _metric_agg_rows() -> list[dict]:
    return [r for r in list_pipeline_results() if r["pipeline_id"] == METRIC_PIPELINE_ID]


def _publish_metric(code: str, new_version: bool = False, **fields) -> dict:
    """发布指标。

    - 编号已存在且不要求新版本：只把最新一条推到「已发布」；
    - ``new_version=True`` 或编号不存在：追加一条新的已发布版本行（最新行生效）。
    """
    rows = store.rows("metric_def")
    existing = [r for r in rows if r.get("指标编号") == code]
    if existing and not new_version:
        row = existing[-1]
        row["status"] = "已发布"
        row["pending"] = False
        return row
    base = dict(existing[-1]) if existing else {
        "指标编号": code,
        "指标名称": code,
        "计算公式": "sum(order_amount)",
        "聚合粒度": "日",
        "时间窗口": "30d",
        "口径版本": "v1.0",
        "数据来源": "order_events",
        "精度位数": "2",
    }
    base.update(fields)
    service = MetricDefService()
    entry, missing = service.create_entry(base)
    assert not missing
    service.run_action(entry["id"], "提交审核")
    entry, _ = service.run_action(entry["id"], "发布指标")
    assert entry["status"] == "已发布"
    return entry


class TestMetricAggVersioning:
    def test_new_published_sum_definition_drives_aggregation(self):
        _publish_metric(
            "METR-ORDER-SUM",
            指标名称="订单额求和",
            计算公式="sum(order_amount)",
            口径版本="v1.0",
            精度位数="2",
        )
        result = run_metric_agg_pipeline("METR-ORDER-SUM")
        assert "error" not in result
        assert result["metric_code"] == "METR-ORDER-SUM"
        assert result["value"] == round(GMV_TOTAL, 2)
        # 一次计算用到的编号、公式和精度随结果一起保留
        spec = result["metric_spec"]
        assert spec["指标编号"] == "METR-ORDER-SUM"
        assert spec["计算公式"] == "sum(order_amount)"
        assert spec["精度位数"] == 2
        assert spec["口径版本"] == "v1.0"
        assert result["run"]["run_seq"] == 1

    def test_unknown_code_keeps_previous_valid_result(self):
        _publish_metric("METR-ORDER-SUM", 口径版本="v1.0", 精度位数="2")
        first = run_metric_agg_pipeline("METR-ORDER-SUM")
        bad = run_metric_agg_pipeline("METR-NOT-EXIST")
        assert "error" in bad
        # 上一条合法结果仍在，且没有任何默认指标（GMV）被借机写入
        record = get_metric_agg_record("METR-ORDER-SUM")
        assert record["value"] == first["value"]
        assert record["metric_spec"]["口径版本"] == "v1.0"
        assert get_metric_agg_record("METR-GMV") is None
        rows = _metric_agg_rows()
        assert len(rows) == 1
        assert len(rows[0]["history"]) == 1

    def test_unpublished_draft_is_not_aggregated(self):
        MetricDefService().create_entry({
            "指标编号": "METR-DRAFT",
            "指标名称": "草稿指标",
            "计算公式": "sum(order_amount)",
            "精度位数": "2",
        })
        result = run_metric_agg_pipeline("METR-DRAFT")
        assert "error" in result
        assert get_metric_agg_record("METR-DRAFT") is None
        assert _metric_agg_rows() == []

    def test_fixture_only_code_is_not_silently_used(self):
        # METR-GMV 在种子数据里是草稿：聚合不能回退到 fixtures 里的同名示例定义
        result = run_metric_agg_pipeline("METR-GMV")
        assert "error" in result
        assert get_metric_agg_record("METR-GMV") is None

    def test_precision_change_preserves_old_snapshot_and_recomputes_new(self):
        _publish_metric(
            "METR-ORDER-SUM", 指标名称="订单额求和",
            计算公式="sum(order_amount)", 口径版本="v1.0", 精度位数="2",
        )
        first = run_metric_agg_pipeline("METR-ORDER-SUM")
        # 更改精度后再次发布；上一次计算已结束并落库
        _publish_metric(
            "METR-ORDER-SUM", new_version=True, 指标名称="订单额求和",
            计算公式="sum(order_amount)", 口径版本="v2.0", 精度位数="4",
        )
        second = run_metric_agg_pipeline("METR-ORDER-SUM")
        # 版本切换后返回值必须按实际生效公式和精度算得出来
        assert second["value"] == round(GMV_TOTAL, 4)
        assert second["metric_spec"]["精度位数"] == 4
        assert second["metric_spec"]["口径版本"] == "v2.0"
        # 老口径数据仍可核对
        old = get_metric_agg_record("METR-ORDER-SUM", "v1.0")
        assert old["value"] == round(GMV_TOTAL, 2)
        assert old["metric_spec"]["精度位数"] == 2
        assert old["metric_spec"]["计算公式"] == "sum(order_amount)"
        # 管道运行详情：两次合法运行按序追加，最新指针指向第二次
        row = _metric_agg_rows()[0]
        assert [h["metric_spec"]["口径版本"] for h in row["history"]] == ["v1.0", "v2.0"]
        latest = get_metric_agg_record("METR-ORDER-SUM")
        assert latest["run_seq"] == second["run"]["run_seq"] == first["run"]["run_seq"] + 1

    def test_returned_value_is_reproducible_from_stored_caliber(self):
        _publish_metric("METR-ORDER-SUM", 口径版本="v3.1", 精度位数="3")
        run_metric_agg_pipeline("METR-ORDER-SUM")
        record = get_metric_agg_record("METR-ORDER-SUM")
        spec = record["metric_spec"]
        recomputed = round(GMV_TOTAL, spec["精度位数"])
        assert record["value"] == recomputed
        # 当前定义再切换版本，数据服务仍按结果自带口径返回老值，不重新贴标签
        _publish_metric("METR-ORDER-SUM", new_version=True, 口径版本="v3.2", 精度位数="1")
        old = get_metric_agg_record("METR-ORDER-SUM", "v3.1")
        assert old["value"] == recomputed
        assert old["metric_spec"]["精度位数"] == 3

    def test_unsupported_formula_does_not_overwrite(self):
        _publish_metric("METR-ORDER-SUM", 口径版本="v1.0", 精度位数="2")
        first = run_metric_agg_pipeline("METR-ORDER-SUM")
        _publish_metric(
            "METR-ORDER-SUM", new_version=True, 口径版本="v9.9", 精度位数="2",
            计算公式="median(unknown_col)",
        )
        bad = run_metric_agg_pipeline("METR-ORDER-SUM")
        assert "error" in bad
        record = get_metric_agg_record("METR-ORDER-SUM")
        assert record["value"] == first["value"]
        assert record["metric_spec"]["口径版本"] == "v1.0"

    def test_engine_returns_error_for_unknown_code(self):
        assert "error" in compute_metric("METR-NOT-EXIST")

    def test_no_published_metric_skips_write(self):
        for row in store.rows("metric_def"):
            if row.get("status") == "已发布":
                row["status"] = "已冻结"
        result = run_metric_agg_pipeline()
        assert "error" in result
        assert list_pipeline_results() == []

    def test_run_all_uses_published_store_definition(self):
        summary = run_all_pipelines()
        assert "error" not in summary["metric_agg_api"]
        code = summary["metric_agg_api"]["metric_code"]
        assert any(
            r.get("指标编号") == code and r.get("status") == "已发布"
            for r in store.rows("metric_def")
        )
        assert len(list_pipeline_results()) == 7


class TestAggTaskBatchCompute:
    def test_failed_run_keeps_previous_task_result(self):
        _publish_metric("METR-ORDER-SUM", 口径版本="v1.0", 精度位数="2")
        task_service = AggTaskService()
        task, missing = task_service.create_entry({
            "任务编号": "AGG-ORDER-SUM",
            "目标指标": "METR-ORDER-SUM",
            "聚合维度": "DIM-REGION",
        })
        assert not missing
        ok = batch_compute_for_agg_task(task["id"])
        assert ok["status"] == "已完成"
        assert ok["compute"]["metric_spec"]["指标编号"] == "METR-ORDER-SUM"

        # 改成不存在的编号重跑：任务失败，但已完成的计算值保留
        task["目标指标"] = "METR-MISSING"
        bad = batch_compute_for_agg_task(task["id"])
        assert "error" in bad
        fresh = store.find("agg_task", task["id"])
        assert fresh["status"] == "计算失败"
        assert fresh["abnormal"] is True
        assert fresh["计算结果"]["metric_code"] == "METR-ORDER-SUM"
        assert fresh["计算结果"]["compute"]["value"] == round(GMV_TOTAL, 2)
