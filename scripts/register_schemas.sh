#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Camada 2 — Contrato de Dados (Stream Catalog Tags + Schemas Avro + Evolução)
# ==============================================================================

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

if [[ -n "${LOCALAPPDATA:-}" ]] && [[ -d "$LOCALAPPDATA/Microsoft/WinGet/Packages" ]]; then
  winget_unix="$(cygpath -u "$LOCALAPPDATA/Microsoft/WinGet/Packages" 2>/dev/null || echo "")"
  if [[ -n "$winget_unix" && -d "$winget_unix" ]]; then
    for bin_path in "$winget_unix"/ConfluentInc.Confluent-CLI*/confluent "$winget_unix"/jqlang.jq*; do
      if [[ -d "$bin_path" && ":$PATH:" != *":$bin_path:"* ]]; then
        export PATH="$PATH:$bin_path"
      fi
    done
  fi
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERRO: .env não encontrado. Execute ./setup.sh primeiro." >&2
  exit 1
fi

# Normaliza CRLF emitido pelo jq.exe no Windows
jq() {
  command jq "$@" | tr -d '\r'
}

while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%$'\r'}"
  [[ -z "$line" || "$line" == \#* ]] && continue
  k="${line%%=*}"
  v="${line#*=}"
  export "$k=$v"
done < "$ENV_FILE"

ENV_ID="${CONFLUENT_ENV_ID:?CONFLUENT_ENV_ID não definido no .env}"
SR_URL="${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL não definido no .env}"
SR_KEY="${SCHEMA_REGISTRY_API_KEY:?SCHEMA_REGISTRY_API_KEY não definido no .env}"
SR_SECRET="${SCHEMA_REGISTRY_API_SECRET:?SCHEMA_REGISTRY_API_SECRET não definido no .env}"

echo "==> [Camada 2] Garantindo definições de tags PII e PCI no Stream Catalog..."
# Cria tagdefs se ainda não existirem (409/already exists é aceito de forma idempotente)
EXISTING_TAGS="$(curl -sS -u "$SR_KEY:$SR_SECRET" "$SR_URL/catalog/v1/types/tagdefs" | jq -r '.[].name' || true)"
if ! grep -qx "PII" <<< "$EXISTING_TAGS" || ! grep -qx "PCI" <<< "$EXISTING_TAGS"; then
  curl -sS -X POST -u "$SR_KEY:$SR_SECRET" \
    -H "Content-Type: application/json" \
    "$SR_URL/catalog/v1/types/tagdefs" \
    -d '[
      {"name":"PII","description":"Personally Identifiable Information"},
      {"name":"PCI","description":"Payment Card Industry Sensitive Data"}
    ]' >/dev/null
  echo "    Tagdefs PII e PCI registradas no Stream Catalog."
else
  echo "    Tagdefs PII e PCI já existem no Stream Catalog."
fi

echo "==> [Camada 2] Registrando contratos Avro em schemas/..."
register_schema() {
  local subject="$1"
  local file="$2"
  local out
  out="$(confluent schema-registry schema create --subject "$subject" --schema "$ROOT_DIR/$file" --type avro --environment "$ENV_ID" -o json)"
  echo "    $subject -> schema ID $(echo "$out" | jq -r '.id')"
}

register_schema "desafio.contracts.customers-value" "schemas/customer.avsc"
register_schema "desafio.contracts.accounts-value" "schemas/account.avsc"
register_schema "desafio.contracts.cards-value" "schemas/card.avsc"
register_schema "desafio.contracts.merchants-value" "schemas/merchant.avsc"
register_schema "desafio.contracts.transactions-value" "schemas/transaction.avsc"
register_schema "desafio.fraud.detected-value" "schemas/fraud-alert.avsc"

echo "==> [Camada 2] Configurando compatibilidade FULL (bidirecional: BACKWARD + FORWARD) em transactions..."
confluent schema-registry subject update "desafio.contracts.transactions-value" --compatibility full --environment "$ENV_ID" >/dev/null

echo "==> [Camada 2] Testando evolução de schema (compatível vs breaking change)..."
echo "  1) Adição de campo novo com default (device_ip -> deve ACEITAR):"
confluent schema-registry schema compatibility validate "$ROOT_DIR/schemas/evolution/transaction-v2-compatible.avsc" \
  --type avro --subject "desafio.contracts.transactions-value" --version latest --environment "$ENV_ID"

echo "  2) Remoção de campo obrigatório sem default (amount -> deve REJEITAR):"
confluent schema-registry schema compatibility validate "$ROOT_DIR/schemas/evolution/transaction-v2-remove-required.avsc" \
  --type avro --subject "desafio.contracts.transactions-value" --version latest --environment "$ENV_ID"

echo "  3) Adição de campo obrigatório sem default (risk_score -> deve REJEITAR):"
confluent schema-registry schema compatibility validate "$ROOT_DIR/schemas/evolution/transaction-v2-add-required.avsc" \
  --type avro --subject "desafio.contracts.transactions-value" --version latest --environment "$ENV_ID"

echo "✅ Camada 2 (Contrato de Dados) verificada!"
