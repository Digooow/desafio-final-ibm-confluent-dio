"""Coleta métricas (Metrics API v2), consumer lag, ACLs e custos da Confluent Cloud."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import requests

from scripts.confluent_utils import resolve_confluent_bin, run_confluent
from scripts.env_utils import load_env

ROOT_DIR = Path(__file__).resolve().parent.parent
METRICS_ENDPOINT = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"


def query_metrics_api(
    api_key: str,
    api_secret: str,
    cluster_id: str,
    metric: str,
    hours_back: int = 4,
    granularity: str = "PT1M",
    group_by: list[str] | None = None,
) -> dict[str, Any]:
    """Consulta a Confluent Cloud Metrics API v2 em janela UTC recente."""
    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(hours=hours_back)
    interval = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    payload: dict[str, Any] = {
        "aggregations": [{"metric": metric}],
        "filter": {"field": "resource.kafka.id", "op": "EQ", "value": cluster_id},
        "granularity": granularity,
        "intervals": [interval],
    }
    if group_by:
        payload["group_by"] = group_by

    resp = requests.post(METRICS_ENDPOINT, json=payload, auth=(api_key, api_secret), timeout=20)
    resp.raise_for_status()
    return cast(dict[str, Any], resp.json())


def _point_value(point: dict[str, Any]) -> float:
    return float(point.get("value", 0))


def _point_timestamp(point: dict[str, Any]) -> str:
    return str(point.get("timestamp", ""))


def summarize_received_bytes(response_json: dict[str, Any]) -> dict[str, Any]:
    """Calcula pico (max), soma total e total de buckets com tráfego em received_bytes."""
    raw_points = response_json.get("data")
    points: list[dict[str, Any]] = (
        cast(list[dict[str, Any]], raw_points) if isinstance(raw_points, list) else []
    )
    non_zero = [p for p in points if _point_value(p) > 0]
    if not points:
        return {"points_count": 0, "non_zero_buckets": 0, "peak_bytes_per_min": 0.0, "total_bytes": 0.0}
    peak = max(points, key=_point_value)
    total = sum(_point_value(p) for p in points)
    return {
        "points_count": len(points),
        "non_zero_buckets": len(non_zero),
        "peak_bytes_per_min": _point_value(peak),
        "peak_timestamp": peak.get("timestamp"),
        "total_bytes": total,
    }


def summarize_consumer_lag(response_json: dict[str, Any]) -> list[dict[str, Any]]:
    """Extrai último e maior lag por consumer group e tópico retornados pela Metrics API."""
    raw_points = response_json.get("data")
    points: list[dict[str, Any]] = (
        cast(list[dict[str, Any]], raw_points) if isinstance(raw_points, list) else []
    )
    by_group_topic: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for pt in points:
        key = (
            str(pt.get("metric.consumer_group_id", "unknown")),
            str(pt.get("metric.topic", "unknown")),
        )
        by_group_topic.setdefault(key, []).append(pt)

    summary: list[dict[str, Any]] = []
    for (group_id, topic), series in sorted(by_group_topic.items()):
        latest = max(series, key=_point_timestamp)
        peak = max(series, key=_point_value)
        summary.append(
            {
                "consumer_group_id": group_id,
                "topic": topic,
                "samples": len(series),
                "latest_lag_offsets": _point_value(latest),
                "latest_timestamp": latest.get("timestamp"),
                "peak_lag_offsets": _point_value(peak),
            }
        )
    return summary


def _run_cli_json(cli: str, args: list[str]) -> Any:
    res = run_confluent(cli, [*args, "-o", "json"])
    if res.returncode != 0:
        return {"error": res.stderr.strip() or res.stdout.strip()}
    text = (res.stdout or "").strip()
    return json.loads(text) if text else []


def main() -> int:
    env = load_env(ROOT_DIR / ".env")
    env_id = env["CONFLUENT_ENV_ID"].strip()
    cluster_id = env["CONFLUENT_CLUSTER_ID"].strip()
    cloud = env.get("CONFLUENT_CLOUD_PROVIDER", "aws").strip()
    region = env.get("CONFLUENT_CLOUD_REGION", "us-east-1").strip()
    pool_id = env.get("FLINK_COMPUTE_POOL_ID", "").strip()
    m_key = env["METRICS_API_KEY"].strip()
    m_secret = env["METRICS_API_SECRET"].strip()

    received_raw = query_metrics_api(
        api_key=m_key,
        api_secret=m_secret,
        cluster_id=cluster_id,
        metric="io.confluent.kafka.server/received_bytes",
        hours_back=4,
    )
    lag_raw = query_metrics_api(
        api_key=m_key,
        api_secret=m_secret,
        cluster_id=cluster_id,
        metric="io.confluent.kafka.server/consumer_lag_offsets",
        hours_back=4,
        group_by=["metric.consumer_group_id", "metric.topic"],
    )

    cli = resolve_confluent_bin()
    acls = _run_cli_json(cli, ["kafka", "acl", "list", "--cluster", cluster_id, "--environment", env_id])
    groups = _run_cli_json(cli, ["kafka", "consumer", "group", "list", "--cluster", cluster_id, "--environment", env_id])

    cli_group_lags: dict[str, Any] = {}
    if isinstance(groups, list):
        for group_item in cast(list[dict[str, Any]], groups):
            gid = group_item.get("consumer_group")
            if isinstance(gid, str) and gid:
                cli_group_lags[gid] = _run_cli_json(
                    cli,
                    ["kafka", "consumer", "group", "lag", "list", gid, "--cluster", cluster_id, "--environment", env_id],
                )

    today = datetime.now(UTC).date()
    start_date = (today - timedelta(days=7)).isoformat()
    end_date = (today + timedelta(days=1)).isoformat()
    billing_costs = _run_cli_json(cli, ["billing", "cost", "list", "--start-date", start_date, "--end-date", end_date])
    prices = _run_cli_json(cli, ["billing", "price", "list", "--cloud", cloud, "--region", region, "--cluster-type", "basic"])
    pool_info: dict[str, Any] = (
        cast(dict[str, Any], _run_cli_json(cli, ["flink", "compute-pool", "describe", pool_id, "--environment", env_id]))
        if pool_id
        else {}
    )
    connectors = _run_cli_json(cli, ["connect", "cluster", "list", "--cluster", cluster_id, "--environment", env_id])

    received_summary = summarize_received_bytes(received_raw)
    lag_summary = summarize_consumer_lag(lag_raw)
    report: dict[str, Any] = {
        "captured_at_utc": datetime.now(UTC).isoformat(),
        "environment_id": env_id,
        "cluster_id": cluster_id,
        "observability": {
            "received_bytes_summary": received_summary,
            "consumer_lag_metrics_api": lag_summary,
            "consumer_group_lag_cli": cli_group_lags,
        },
        "security_acls": acls,
        "costs_and_capacity": {
            "billing_cost_window": f"{start_date}..{end_date}",
            "billing_cost_entries": billing_costs,
            "flink_compute_pool": pool_info,
            "connectors": connectors,
            "basic_unit_prices": prices,
        },
    }

    out_path = ROOT_DIR / "evidencias" / "03-observabilidade-custos.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("OBSERVABILITY_OK:", json.dumps(received_summary))
    print("LAG_API_COUNT:", len(lag_summary))
    print("SAVED:", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
