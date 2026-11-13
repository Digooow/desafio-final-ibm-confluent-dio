-- =============================================================================
-- Camada 4 (Flink SQL) — Configuração de PKs, Watermarks e Changelog Mode
-- =============================================================================
-- Motivação arquitetural:
--   1. Temporal Join (transactions ⋈ accounts FOR SYSTEM_TIME AS OF t.created_at)
--      exige PRIMARY KEY NOT ENFORCED em ambos os lados para semântica versionada.
--      (`desafio.cdc.public.accounts` já possui PK em account_id via tópico compactado).
--   2. A carga inicial (2.000 transações) entra via Debezium snapshot em ~2 segundos
--      de relógio ($rowtime), o que geraria centenas de falsos alertas de velocidade.
--      Definir WATERMARK no tempo de evento de negócio (created_at / updated_at) com
--      atraso tolerado de 5s garante determinismo e zero falsos positivos no snapshot.
--   3. `desafio.cdc.public.transactions` é append-only no domínio e usa
--      `'changelog.mode' = 'append'` para que o Temporal Join produza fluxo append-only
--      compatível com MATCH_RECOGNIZE (que não consome stream retract/delete).
-- =============================================================================

ALTER TABLE `desafio.cdc.public.transactions`
  ADD CONSTRAINT pk_transactions PRIMARY KEY (transaction_id) NOT ENFORCED;

ALTER TABLE `desafio.cdc.public.transactions`
  MODIFY WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND;

ALTER TABLE `desafio.cdc.public.transactions`
  SET ('changelog.mode' = 'append');

ALTER TABLE `desafio.cdc.public.accounts`
  MODIFY WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND;
