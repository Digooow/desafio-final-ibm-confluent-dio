"""Testes unitários para agregação de métricas e lag da Camada 5."""

from __future__ import annotations

from scripts.collect_observability import summarize_consumer_lag, summarize_received_bytes


def test_summarize_received_bytes_handles_empty_and_peak() -> None:
    assert summarize_received_bytes({})["peak_bytes_per_min"] == 0.0

    sample = {
        "data": [
            {"timestamp": "2026-11-13T06:56:00Z", "value": 100.0},
            {"timestamp": "2026-11-13T06:57:00Z", "value": 507577.0},
            {"timestamp": "2026-11-13T06:58:00Z", "value": 0.0},
        ]
    }
    res = summarize_received_bytes(sample)
    assert res["points_count"] == 3
    assert res["non_zero_buckets"] == 2
    assert res["peak_bytes_per_min"] == 507577.0
    assert res["peak_timestamp"] == "2026-11-13T06:57:00Z"
    assert res["total_bytes"] == 507677.0


def test_summarize_consumer_lag_groups_and_selects_latest() -> None:
    sample = {
        "data": [
            {
                "metric.consumer_group_id": "desafio-fraud-consumer",
                "metric.topic": "desafio.fraud.detected",
                "timestamp": "2026-11-13T07:17:00Z",
                "value": 2.0,
            },
            {
                "metric.consumer_group_id": "desafio-fraud-consumer",
                "metric.topic": "desafio.fraud.detected",
                "timestamp": "2026-11-13T07:26:00Z",
                "value": 1.0,
            },
        ]
    }
    res = summarize_consumer_lag(sample)
    assert len(res) == 1
    assert res[0]["latest_lag_offsets"] == 1.0
    assert res[0]["peak_lag_offsets"] == 2.0
