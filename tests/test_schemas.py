import json
from pathlib import Path

import fastavro
import pytest

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
CORE_SCHEMAS = [
    "customer.avsc",
    "account.avsc",
    "card.avsc",
    "merchant.avsc",
    "transaction.avsc",
    "fraud-alert.avsc",
]


@pytest.mark.parametrize("filename", CORE_SCHEMAS)
def test_schema_is_valid_avro_and_uses_expected_namespace(filename: str) -> None:
    raw = json.loads((SCHEMAS_DIR / filename).read_text(encoding="utf-8"))
    assert raw["namespace"] == "com.bootcamp.payments"
    parsed = fastavro.parse_schema(raw)
    assert isinstance(parsed, dict)
    assert parsed["name"] == f"{raw['namespace']}.{raw['name']}"


@pytest.mark.parametrize(
    "filename",
    [
        "evolution/transaction-v2-compatible.avsc",
        "evolution/transaction-v2-remove-required.avsc",
        "evolution/transaction-v2-add-required.avsc",
    ],
)
def test_evolution_schemas_parse_cleanly(filename: str) -> None:
    raw = json.loads((SCHEMAS_DIR / filename).read_text(encoding="utf-8"))
    parsed = fastavro.parse_schema(raw)
    assert isinstance(parsed, dict)
    assert parsed["name"] == "com.bootcamp.payments.Transaction"


def test_monetary_fields_use_decimal_15_2_never_float() -> None:
    monetary_expectations = {
        "account.avsc": "balance",
        "transaction.avsc": "amount",
        "fraud-alert.avsc": "total_amount",
    }
    for filename, field_name in monetary_expectations.items():
        raw = json.loads((SCHEMAS_DIR / filename).read_text(encoding="utf-8"))
        fields = {f["name"]: f for f in raw["fields"]}
        field_type = fields[field_name]["type"]
        assert isinstance(field_type, dict), f"{filename}.{field_name} deve usar tipo lógico decimal"
        assert field_type["type"] == "bytes"
        assert field_type["logicalType"] == "decimal"
        assert field_type["precision"] == 15
        assert field_type["scale"] == 2


def test_sensitive_fields_carry_pii_or_pci_tags() -> None:
    expected_tags = {
        "customer.avsc": {"full_name": "PII", "email": "PII", "document_id": "PII"},
        "card.avsc": {"card_fingerprint": "PCI", "last4": "PCI"},
        "transaction.avsc": {"card_id": "PCI"},
        "fraud-alert.avsc": {"card_id": "PCI"},
    }
    for filename, field_tags in expected_tags.items():
        raw = json.loads((SCHEMAS_DIR / filename).read_text(encoding="utf-8"))
        fields = {f["name"]: f for f in raw["fields"]}
        for field_name, required_tag in field_tags.items():
            tags = fields[field_name].get("confluent:tags", [])
            assert required_tag in tags, f"{filename}.{field_name} deve conter tag {required_tag}"


def test_card_schema_never_exposes_raw_pan() -> None:
    raw = json.loads((SCHEMAS_DIR / "card.avsc").read_text(encoding="utf-8"))
    field_names = {f["name"] for f in raw["fields"]}
    assert not {"pan", "card_number", "cvv", "cvc"} & field_names
