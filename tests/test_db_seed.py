"""Testes unitários de integridade, determinismo e contratos SQL do seed (Camada 3)."""

from __future__ import annotations

import re
from pathlib import Path

from scripts.seed_db import EXPECTED_COUNTS, generate_seed_data

ROOT_DIR = Path(__file__).resolve().parent.parent


def test_seed_row_counts_and_determinism() -> None:
    ds_a = generate_seed_data(seed=42)
    ds_b = generate_seed_data(seed=42)
    ds_other = generate_seed_data(seed=43)

    assert len(ds_a.customers) == EXPECTED_COUNTS["customers"]
    assert len(ds_a.accounts) == EXPECTED_COUNTS["accounts"]
    assert len(ds_a.cards) == EXPECTED_COUNTS["cards"]
    assert len(ds_a.merchants) == EXPECTED_COUNTS["merchants"]
    assert len(ds_a.transactions) == EXPECTED_COUNTS["transactions"]

    assert ds_a == ds_b
    assert ds_a != ds_other


def test_seed_referential_and_domain_invariants() -> None:
    ds = generate_seed_data(seed=42)

    customer_ids = {c[0] for c in ds.customers}
    account_ids = {a[0] for a in ds.accounts}
    card_account_map = {c[0]: c[1] for c in ds.cards}
    merchant_ids = {m[0] for m in ds.merchants}

    assert len({c[2] for c in ds.customers}) == len(ds.customers)
    assert len({c[3] for c in ds.customers}) == len(ds.customers)
    assert len({c[2] for c in ds.cards}) == len(ds.cards)

    for _, cust_id, status, balance, currency, _ in ds.accounts:
        assert cust_id in customer_ids
        assert status in {"ACTIVE", "BLOCKED", "CLOSED"}
        assert balance >= 0
        assert balance.as_tuple().exponent == -2
        assert currency == "BRL"

    for _, acc_id, fp, last4, brand, status, _ in ds.cards:
        assert acc_id in account_ids
        assert re.fullmatch(r"[0-9a-f]{64}", fp)
        assert re.fullmatch(r"\d{4}", last4)
        assert brand in {"VISA", "MASTERCARD", "ELO"}
        assert status in {"ACTIVE", "FROZEN", "CANCELLED"}

    for _, acc_id, card_id, merch_id, amount, currency, status, channel, _ in ds.transactions:
        assert acc_id in account_ids
        assert card_id in card_account_map
        assert card_account_map[card_id] == acc_id
        assert merch_id in merchant_ids
        assert amount > 0
        assert amount.as_tuple().exponent == -2
        assert currency == "BRL"
        assert status in {"APPROVED", "DECLINED", "PENDING"}
        assert channel in {"POS", "ECOMMERCE", "CONTACTLESS"}


def test_schema_sql_enforces_replica_identity_full_and_publication() -> None:
    sql_text = (ROOT_DIR / "db" / "01_schema.sql").read_text(encoding="utf-8")
    for table in ("customers", "accounts", "cards", "merchants", "transactions"):
        assert f"ALTER TABLE public.{table} REPLICA IDENTITY FULL;" in sql_text
    assert "CREATE PUBLICATION dbz_publication FOR TABLE" in sql_text
