"""预测分析引擎：基于时序数据执行真实预测计算，供 forecast 与 report_gen 消费。"""
from __future__ import annotations

import statistics
from datetime import date, datetime, timedelta
from typing import Any

from app.fixtures import TREND_POINTS
from app.store import store


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


def get_trend_series(metric_code: str) -> list[dict[str, Any]]:
    """从 trend_series 模块查找对应指标时序。"""
    series = [s for s in store.rows("trend_series") if s.get("观测指标") == metric_code]
    return [dict(s) for s in series]


def linear_forecast(values: list[float], horizon: int = 7) -> dict[str, Any]:
    """线性趋势外推。"""
    n = len(values)
    if n < 2:
        return {"error": "数据不足"}

    xs = list(range(n))
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(values)
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    slope = num / den if den else 0.0
    intercept = y_mean - slope * x_mean

    predictions = []
    for i in range(1, horizon + 1):
        pred = slope * (n - 1 + i) + intercept
        predictions.append(round(pred, 2))

    return {
        "model": "linear_trend",
        "slope": round(slope, 4),
        "intercept": round(intercept, 2),
        "predictions": predictions,
        "r2": round(1 - sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, values)) / sum((y - y_mean) ** 2 for y in values), 4) if sum((y - y_mean) ** 2 for y in values) else 1.0,
    }


def moving_average_forecast(values: list[float], window: int = 7, horizon: int = 14) -> dict[str, Any]:
    """移动平均预测。"""
    n = len(values)
    if n < window:
        return {"error": "数据不足"}

    last_window = values[-window:]
    avg = statistics.mean(last_window)
    predictions = [round(avg, 2)] * horizon

    # 计算 MAPE
    mape = sum(abs(values[i] - statistics.mean(values[max(0, i - window):i + 1])) / values[i] for i in range(window, n) if values[i]) / max(n - window, 1)

    return {
        "model": "moving_average",
        "window": window,
        "predictions": predictions,
        "mape": round(mape, 4),
    }


def exponential_smoothing_forecast(values: list[float], alpha: float = 0.3, horizon: int = 7) -> dict[str, Any]:
    """指数平滑预测。"""
    n = len(values)
    if n < 2:
        return {"error": "数据不足"}

    smoothed = [values[0]]
    for v in values[1:]:
        smoothed.append(alpha * v + (1 - alpha) * smoothed[-1])

    last_smoothed = smoothed[-1]
    predictions = [round(last_smoothed, 2)] * horizon

    # 简单趋势调整
    trend = last_smoothed - smoothed[-2] if len(smoothed) >= 2 else 0.0
    for i in range(horizon):
        predictions[i] = round(last_smoothed + trend * (i + 1), 2)

    # 计算 MSE
    mse = sum((values[i] - smoothed[i]) ** 2 for i in range(n)) / n

    return {
        "model": "exponential_smoothing",
        "alpha": alpha,
        "predictions": predictions,
        "mse": round(mse, 2),
    }


def run_forecast_for_metric(metric_code: str, algorithm: str | None = None, horizon: int = 7) -> dict[str, Any]:
    """为指定指标执行预测计算，自动选择算法或按传入算法执行。"""
    # 映射指标到数值列
    if "GMV" in metric_code or "gmv" in metric_code.lower():
        values = [p["gmv"] for p in TREND_POINTS]
    elif "UV" in metric_code or "uv" in metric_code.lower():
        values = [float(p["uv"]) for p in TREND_POINTS]
    elif "CVR" in metric_code or "cvr" in metric_code.lower():
        values = [p["paid_uv"] / p["total_uv"] if p["total_uv"] else 0.0 for p in TREND_POINTS]
    else:
        values = [p["gmv"] for p in TREND_POINTS]

    # 查找 forecast 记录
    forecasts = [f for f in store.rows("forecast") if f.get("预测指标") == metric_code]
    if forecasts and algorithm is None:
        algorithm = str(forecasts[0].get("预测算法", "线性趋势"))

    if algorithm is None or "线性" in algorithm:
        result = linear_forecast(values, horizon)
    elif "移动平均" in algorithm or "移动" in algorithm:
        result = moving_average_forecast(values, 7, horizon)
    elif "指数平滑" in algorithm or "平滑" in algorithm:
        result = exponential_smoothing_forecast(values, 0.3, horizon)
    else:
        result = linear_forecast(values, horizon)

    result["metric_code"] = metric_code
    result["horizon"] = horizon
    result["actual_count"] = len(values)
    result["last_actual"] = round(values[-1], 2) if values else 0.0

    # 生成预测日期
    last_date = datetime.strptime(TREND_POINTS[-1]["日期"], "%Y-%m-%d").date()
    result["forecast_dates"] = [(last_date + timedelta(days=i)).isoformat() for i in range(1, horizon + 1)]

    return result


def batch_forecast_all() -> dict[str, Any]:
    """为所有 forecast 记录执行预测并返回汇总。"""
    results: dict[str, Any] = {}
    for fc in store.rows("forecast"):
        metric_code = str(fc.get("预测指标", ""))
        algorithm = str(fc.get("预测算法", "线性趋势"))
        horizon = _to_int(fc.get("预测周期", "7").replace("天", ""))
        if metric_code:
            results[metric_code] = run_forecast_for_metric(metric_code, algorithm, horizon)
    return results
