-- =============================================================================
-- Camada 4 (Flink SQL) — Enriquecimento (Temporal Join) + Regra RULE_VELOCITY
-- =============================================================================
-- Executar com: --property sql.tables.scan.idle-timeout=5s
-- O timeout de ociosidade em 5s evita que a partição sem novos eventos na tabela
-- versionada `accounts` trave o avanço da watermark do Temporal Join.
-- =============================================================================

INSERT INTO `desafio.fraud.detected` (
  `key`,
  alert_id,
  card_id,
  account_id,
  rule_name,
  tx_count,
  total_amount,
  window_start,
  window_end
)
SELECT
  ENCODE(
    CONCAT(
      'RULE_VELOCITY:',
      CAST(card_id AS STRING),
      ':',
      CAST(first_tx_id AS STRING)
    ),
    'UTF-8'
  ) AS `key`,
  CONCAT(
    'RULE_VELOCITY:',
    CAST(card_id AS STRING),
    ':',
    CAST(first_tx_id AS STRING)
  ) AS alert_id,
  card_id,
  account_id,
  'RULE_VELOCITY' AS rule_name,
  tx_count,
  total_amount,
  window_start,
  window_end
FROM (
  SELECT
    t.transaction_id,
    t.card_id,
    t.account_id,
    t.amount,
    t.created_at
  FROM `desafio.cdc.public.transactions` AS t
  JOIN `desafio.cdc.public.accounts` FOR SYSTEM_TIME AS OF t.created_at AS a
    ON t.account_id = a.account_id
  WHERE t.status = 'APPROVED'
    AND a.status = 'ACTIVE'
)
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY created_at
  MEASURES
    FIRST(T.transaction_id) AS first_tx_id,
    FIRST(T.account_id) AS account_id,
    COUNT(T.transaction_id) AS tx_count,
    SUM(T.amount) AS total_amount,
    FIRST(T.created_at) AS window_start,
    LAST(T.created_at) AS window_end
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE T AS TRUE
);
