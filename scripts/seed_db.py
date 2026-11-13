"""Carga inicial determinística (seed=42) para as 5 tabelas transacionais no Neon Postgres."""

from __future__ import annotations

import argparse
import hashlib
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import psycopg
from psycopg import sql

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.env_utils import load_env

EXPECTED_COUNTS = {
    "customers": 200,
    "accounts": 240,
    "cards": 320,
    "merchants": 60,
    "transactions": 2000,
}

BASE_UTC = datetime(2026, 11, 1, 12, 0, 0)

FIRST_NAMES = (
    "Ana", "Bruno", "Camila", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique",
    "Isabela", "Joao", "Larissa", "lucas", "Mariana", "Nathan", "Olivia", "Pedro",
    "Rafaela", "Rafael", "Sofia", "Thiago", "Vanessa", "Vitor", "Yasmin", "Zeca",
)
LAST_NAMES = (
    "Almeida", "Barbosa", "Cardoso", "Dias", "Esteves", "Ferreira", "Gomes", "Lima",
    "Martins", "Nascimento", "Oliveira", "Pereira", "Ribeiro", "Santos", "Silva", "Souza",
)
MERCHANT_CATEGORIES = (
    ("Supermercado", "5411"),
    ("Eletronicos", "5732"),
    ("Restaurante", "5812"),
    ("Combustivel", "5541"),
    ("Farmacia", "5912"),
    ("Marketplace", "5399"),
    ("Viagens", "4722"),
    ("Vestuario", "5651"),
)
BRANDS = ("VISA", "MASTERCARD", "ELO")
CHANNELS = ("POS", "ECOMMERCE", "CONTACTLESS")


@dataclass(frozen=True)
class SeedDataset:
    customers: list[tuple[int, str, str, str, str, datetime]]
    accounts: list[tuple[int, int, str, Decimal, str, datetime]]
    cards: list[tuple[int, int, str, str, str, str, datetime]]
    merchants: list[tuple[int, str, str, str, str]]
    transactions: list[tuple[int, int, int, int, Decimal, str, str, str, datetime]]


def _money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def generate_seed_data(seed: int = 42) -> SeedDataset:
    """Gera conjunto determinístico e referencialmente íntegro para carga CDC inicial."""
    rng = random.Random(seed)

    customers: list[tuple[int, str, str, str, str, datetime]] = []
    for cid in range(1, EXPECTED_COUNTS["customers"] + 1):
        full_name = f"{rng.choice(FIRST_NAMES).title()} {rng.choice(LAST_NAMES)}"
        email = f"cliente.{cid:04d}@example.test"
        # Permutação bijetiva mod 999983 garante document_id mascarado único e determinístico
        masked_num = (cid * 48271) % 999983
        document_id = f"***-{masked_num:06d}-**"
        risk_tier = rng.choices(("LOW", "MEDIUM", "HIGH"), weights=(70, 22, 8), k=1)[0]
        created_at = BASE_UTC + timedelta(minutes=cid * 3)
        customers.append((cid, full_name, email, document_id, risk_tier, created_at))

    # Garante ao menos 1 conta por cliente e distribui as restantes
    account_customer_ids = list(range(1, EXPECTED_COUNTS["customers"] + 1)) + [
        rng.randint(1, EXPECTED_COUNTS["customers"])
        for _ in range(EXPECTED_COUNTS["accounts"] - EXPECTED_COUNTS["customers"])
    ]
    accounts: list[tuple[int, int, str, Decimal, str, datetime]] = []
    for aid, cust_id in enumerate(account_customer_ids, start=1):
        status = rng.choices(("ACTIVE", "BLOCKED", "CLOSED"), weights=(90, 6, 4), k=1)[0]
        balance = _money(rng.uniform(500.0, 25000.0))
        updated_at = BASE_UTC + timedelta(hours=1, minutes=aid * 2)
        accounts.append((aid, cust_id, status, balance, "BRL", updated_at))

    # Garante ao menos 1 cartão por conta e distribui os restantes
    card_account_ids = list(range(1, EXPECTED_COUNTS["accounts"] + 1)) + [
        rng.randint(1, EXPECTED_COUNTS["accounts"])
        for _ in range(EXPECTED_COUNTS["cards"] - EXPECTED_COUNTS["accounts"])
    ]
    cards: list[tuple[int, int, str, str, str, str, datetime]] = []
    card_to_account: dict[int, int] = {}
    for card_id, acc_id in enumerate(card_account_ids, start=1):
        card_to_account[card_id] = acc_id
        fingerprint = hashlib.sha256(f"seed:{seed}:card:{card_id}".encode()).hexdigest()
        last4 = f"{rng.randint(0, 9999):04d}"
        brand = rng.choice(BRANDS)
        status = rng.choices(("ACTIVE", "FROZEN", "CANCELLED"), weights=(88, 8, 4), k=1)[0]
        created_at = BASE_UTC + timedelta(hours=2, minutes=card_id)
        cards.append((card_id, acc_id, fingerprint, last4, brand, status, created_at))

    merchants: list[tuple[int, str, str, str, str]] = []
    for mid in range(1, EXPECTED_COUNTS["merchants"] + 1):
        label, mcc = rng.choice(MERCHANT_CATEGORIES)
        name = f"{label} Brasil #{mid:02d}"
        country = "BR" if rng.random() < 0.92 else rng.choice(("US", "AR", "UY"))
        risk_level = rng.choices(("LOW", "MEDIUM", "HIGH"), weights=(75, 18, 7), k=1)[0]
        merchants.append((mid, name, mcc, country, risk_level))

    transactions: list[tuple[int, int, int, int, Decimal, str, str, str, datetime]] = []
    # Transações históricas com espaçamento >= 2 minutos no seed para evitar falsos alertas históricos
    tx_cursor = BASE_UTC + timedelta(days=1)
    for tid in range(1, EXPECTED_COUNTS["transactions"] + 1):
        card_id = rng.randint(1, EXPECTED_COUNTS["cards"])
        acc_id = card_to_account[card_id]
        merch_id = rng.randint(1, EXPECTED_COUNTS["merchants"])
        amount = _money(rng.uniform(12.0, 3800.0))
        status = rng.choices(("APPROVED", "DECLINED", "PENDING"), weights=(86, 10, 4), k=1)[0]
        channel = rng.choice(CHANNELS)
        tx_cursor += timedelta(minutes=2, seconds=rng.randint(5, 45))
        transactions.append((tid, acc_id, card_id, merch_id, amount, "BRL", status, channel, tx_cursor))

    return SeedDataset(
        customers=customers,
        accounts=accounts,
        cards=cards,
        merchants=merchants,
        transactions=transactions,
    )


def _fetch_counts(cur: psycopg.Cursor) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in EXPECTED_COUNTS:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM public.{};").format(sql.Identifier(table)))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Consulta COUNT(*) em public.{table} não retornou linha.")
        counts[table] = int(row[0])
    return counts


def load_seed_into_db(conn: psycopg.Connection, dataset: SeedDataset, reset: bool = False) -> dict[str, int]:
    """Insere os dados determinísticos em transação única e sincroniza sequências IDENTITY."""
    with conn.cursor() as cur:
        current = _fetch_counts(cur)
        if not reset and current == EXPECTED_COUNTS:
            return current
        if not reset and any(v > 0 for v in current.values()):
            raise RuntimeError(
                f"Banco já contém dados parciais {current}; use --reset para recarregar o seed."
            )

        if reset:
            cur.execute(
                "TRUNCATE TABLE public.transactions, public.cards, public.accounts, public.merchants, public.customers RESTART IDENTITY CASCADE;"
            )

        cur.executemany(
            """
            INSERT INTO public.customers (customer_id, full_name, email, document_id, risk_tier, created_at)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            dataset.customers,
        )
        cur.executemany(
            """
            INSERT INTO public.accounts (account_id, customer_id, status, balance, currency, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            dataset.accounts,
        )
        cur.executemany(
            """
            INSERT INTO public.cards (card_id, account_id, card_fingerprint, last4, brand, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            dataset.cards,
        )
        cur.executemany(
            """
            INSERT INTO public.merchants (merchant_id, name, category_code, country, risk_level)
            VALUES (%s, %s, %s, %s, %s);
            """,
            dataset.merchants,
        )
        cur.executemany(
            """
            INSERT INTO public.transactions (transaction_id, account_id, card_id, merchant_id, amount, currency, status, channel, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            dataset.transactions,
        )

        seq_specs = (
            ("public.customers", "customer_id"),
            ("public.accounts", "account_id"),
            ("public.cards", "card_id"),
            ("public.merchants", "merchant_id"),
            ("public.transactions", "transaction_id"),
        )
        for tbl, pk in seq_specs:
            schema_name, table_name = tbl.split(".", 1)
            cur.execute(
                sql.SQL(
                    "SELECT setval(pg_get_serial_sequence({}, {}), COALESCE((SELECT MAX({}) FROM {}.{}), 1), true);"
                ).format(
                    sql.Literal(tbl),
                    sql.Literal(pk),
                    sql.Identifier(pk),
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                )
            )

        final_counts = _fetch_counts(cur)
    conn.commit()
    return final_counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Carrega seed determinístico no Neon Postgres.")
    parser.add_argument("--seed", type=int, default=42, help="Semente aleatória determinística (padrão: 42).")
    parser.add_argument("--reset", action="store_true", help="Trunca e recarrega todas as tabelas antes de inserir.")
    args = parser.parse_args()

    env = load_env(ROOT_DIR / ".env")
    unpooled_url = env.get("DATABASE_URL_UNPOOLED", "").strip()
    if not unpooled_url:
        raise SystemExit("ERRO: DATABASE_URL_UNPOOLED não encontrada no .env.")

    dataset = generate_seed_data(seed=args.seed)
    with psycopg.connect(unpooled_url, connect_timeout=30) as conn:
        counts = load_seed_into_db(conn, dataset, reset=args.reset)

    header = " | ".join(f"{t}" for t in EXPECTED_COUNTS)
    values = " | ".join(f"{counts[t]:>{len(t)}}" for t in EXPECTED_COUNTS)
    print(header)
    print("-" * len(header))
    print(values)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
