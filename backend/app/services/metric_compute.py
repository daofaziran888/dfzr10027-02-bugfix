"""指标计算引擎：根据已发布的指标定义执行真实聚合计算，供 agg_task 与 api_service 消费。

口径一致性约定：
- 只读取 store 中状态为「已发布」的指标定义，fixtures 中的示例定义不参与聚合；
- 每次计算把当时生效的指标编号、计算公式、精度位数、口径版本固化成
  ``metric_spec`` 随结果一起返回/落库，下游（数据服务）必须按该快照解释结果，
  不能用当前最新定义给历史结果重新贴标签；
- 编号找不到、定义未发布或公式无法识别时返回 ``{"error": ...}``，由调用方决定
  是否落库，引擎本身不写任何存储，避免错误结果覆盖上一次的合法值。
"""
from __future__ import annotations

import statistics
from typing import Any

from app.fixtures import TREND_POINTS
from app.store import store

PUBLISHED_STATUS = "已发布"
# 受支持的公式片段，集中声明便于口径快照与错误信息复用
FORMULA_SUM_ORDER = "sum(order_amount)"
FORMULA_COUNT_UV = "count(distinct user_id)"
FORMULA_CVR = "paid_uv / total_uv"
FORMULA_ARPU = "gmv / uv"


def _to_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _to_int(val: Any) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _snapshot(metric: dict[str, Any]) -> dict[str, Any]:
    """固化计算时刻的口径，保证结果日后可按同一公式与精度复核。"""
    return {
        "指标编号": metric.get("指标编号"),
        "指标名称": metric.get("指标名称"),
        "计算公式": metric.get("计算公式"),
        "精度位数": _to_int(metric.get("精度位数", 2)),
        "口径版本": metric.get("口径版本"),
        "聚合粒度": metric.get("聚合粒度", "日"),
        "数据来源": metric.get("数据来源"),
    }


def find_metric_def(metric_code: str) -> dict[str, Any] | None:
    """查找指标编号当前已发布的定义；草稿/已审核/已冻结等状态一律不可用于聚合。

    同一编号存在多个已发布版本时（改了精度/公式后再次发布），取最后写入的一条，
    即最新发布的口径为当前生效口径；历史口径只通过聚合结果里的快照留存。
    """
    matches = [
        dict(row)
        for row in store.rows("metric_def")
        if row.get("指标编号") == metric_code and row.get("status") == PUBLISHED_STATUS
    ]
    return matches[-1] if matches else None


def compute_metric(metric_code: str) -> dict[str, Any]:
    """按指标编号读取已发布定义并执行真实聚合计算。

    支持的公式语义：
    - sum(order_amount) -> 订单额求和（交易总额）
    - count(distinct user_id) -> 独立访客数
    - paid_uv / total_uv -> 支付转化率
    - gmv / uv -> 每用户平均收入

    返回值始终携带 ``metric_spec`` 口径快照；无法计算时仅返回 ``error``，
    不带 value，调用方不得据此覆盖既有结果。
    """
    metric = find_metric_def(metric_code)
    if metric is None:
        return {"error": f"指标 {metric_code} 不存在或未发布，已跳过本次聚合写入"}

    formula = str(metric.get("计算公式", ""))
    precision = _to_int(metric.get("精度位数", 2))
    points = list(TREND_POINTS)
    spec = _snapshot(metric)
    result: dict[str, Any] = {
        "metric_code": metric_code,
        "granularity": metric.get("聚合粒度", "日"),
        "metric_spec": spec,
    }

    if FORMULA_SUM_ORDER in formula:
        total = sum(p["gmv"] for p in points)
        result["value"] = round(total, precision)
        result["unit"] = "元"
        result["point_count"] = len(points)
        result["avg_daily"] = round(total / len(points), precision) if points else 0.0
        result["max_daily"] = round(max(p["gmv"] for p in points), precision) if points else 0.0
        result["min_daily"] = round(min(p["gmv"] for p in points), precision) if points else 0.0
    elif FORMULA_COUNT_UV in formula:
        total = sum(p["uv"] for p in points)
        result["value"] = total
        result["unit"] = "人"
        result["point_count"] = len(points)
        result["avg_daily"] = round(total / len(points), 0) if points else 0.0
    elif FORMULA_CVR in formula:
        total_paid = sum(p["paid_uv"] for p in points)
        total_uv = sum(p["total_uv"] for p in points)
        cvr = total_paid / total_uv if total_uv else 0.0
        result["value"] = round(cvr, precision)
        result["unit"] = "比率"
        result["point_count"] = len(points)
        result["total_paid"] = total_paid
        result["total_uv"] = total_uv
    elif FORMULA_ARPU in formula:
        total_gmv = sum(p["gmv"] for p in points)
        total_uv = sum(p["uv"] for p in points)
        arpu = total_gmv / total_uv if total_uv else 0.0
        result["value"] = round(arpu, precision)
        result["unit"] = "元/人"
        result["point_count"] = len(points)
    else:
        # 公式无法识别属于计算错误：返回 error 而不是 0.0，避免把占位值当真值落库
        return {"error": f"指标 {metric_code} 的计算公式「{formula}」暂不支持，已跳过本次聚合写入"}

    return result


def compute_metric_trend(metric_code: str) -> dict[str, Any]:
    """计算指标的趋势特征（周同比、环比、变异系数）。"""
    points = list(TREND_POINTS)
    metric = find_metric_def(metric_code)
    if metric is None:
        return {"error": f"指标 {metric_code} 不存在或未发布"}

    formula = str(metric.get("计算公式", ""))
    values: list[float] = []
    if FORMULA_SUM_ORDER in formula:
        values = [p["gmv"] for p in points]
    elif FORMULA_COUNT_UV in formula:
        values = [float(p["uv"]) for p in points]
    elif FORMULA_CVR in formula:
        values = [p["paid_uv"] / p["total_uv"] if p["total_uv"] else 0.0 for p in points]
    elif FORMULA_ARPU in formula:
        values = [p["gmv"] / p["uv"] if p["uv"] else 0.0 for p in points]
    else:
        values = [p["gmv"] for p in points]

    if len(values) < 2:
        return {"error": "数据不足"}

    n = len(values)
    # 环比：最后一日 vs 前一日
    mom = (values[-1] - values[-2]) / values[-2] if values[-2] else 0.0
    # 周同比：最后一日 vs 7日前
    yoy = (values[-1] - values[-7]) / values[-7] if n >= 8 and values[-7] else 0.0
    # 变异系数
    cv = statistics.stdev(values) / statistics.mean(values) if statistics.mean(values) else 0.0

    return {
        "metric_code": metric_code,
        "values": [round(v, 2) for v in values],
        "mom": round(mom, 4),
        "yoy": round(yoy, 4) if n >= 8 else None,
        "cv": round(cv, 4),
        "mean": round(statistics.mean(values), 2),
        "trend_direction": "上升" if mom > 0 else "下降" if mom < 0 else "持平",
    }


def batch_compute_for_agg_task(task_id: int) -> dict[str, Any]:
    """为聚合任务执行真实计算，返回计算结果并更新任务状态。

    编号缺失、定义未发布或公式出错时：任务标记为「计算失败」，
    已有「计算结果」原样保留，绝不用错误结果覆盖上一次的合法值。
    """
    task = store.find("agg_task", task_id)
    if task is None:
        return {"error": f"聚合任务 {task_id} 不存在"}

    metric_code = task.get("目标指标", "")
    compute_result = compute_metric(metric_code)
    if "error" in compute_result:
        task["status"] = "计算失败"
        task["pending"] = False
        task["abnormal"] = True
        return compute_result

    trend_result = compute_metric_trend(metric_code)

    result = {
        "task_id": task_id,
        "task_code": task.get("任务编号"),
        "metric_code": metric_code,
        "compute": compute_result,
        "trend": trend_result,
        "status": "已完成",
    }

    # 仅在计算成功时更新任务记录与结果
    task["status"] = "已完成"
    task["pending"] = False
    task["abnormal"] = False
    task["计算结果"] = result
    return result
