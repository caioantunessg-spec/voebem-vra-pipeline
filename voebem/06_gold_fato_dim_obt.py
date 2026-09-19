# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold — fato, dimensão e OBT
# MAGIC %md
# MAGIC # Gold — fato, dimensão e OBT
# MAGIC
# MAGIC A gold é onde as regras de negócio moram. A silver era espelho; a gold é decisão.
# MAGIC
# MAGIC Três tabelas saem daqui:
# MAGIC
# MAGIC 1. `fato_voos` — uma linha por etapa, com companhia e códigos de operação como dimensões degeneradas. É aqui que nascem as regras: pontualidade a 15 minutos, escopo doméstico/internacional, nulificação de atraso fora de faixa.
# MAGIC 2. `dim_aeroporto` — dimensão de aeroporto construída a partir dos códigos presentes no fato, enriquecida pelo cadastro da ANAC. Cobre 100% do fato, inclusive aeroportos estrangeiros.
# MAGIC 3. `obt_voos` — One Big Table desnormalizada, desenhada para consumo por agente de IA. Sem nenhum JOIN.
# MAGIC
# MAGIC Contagem = `silver.vra` menos duplicatas exatas.

# COMMAND ----------

# DBTITLE 1,fato_voos — dedup, enriquecimento e regras de negócio
# MAGIC %md
# MAGIC ## 1. `fato_voos` — dedup, enriquecimento e regras de negócio
# MAGIC
# MAGIC O ponto de partida é `silver.vra` inteira. Três coisas acontecem aqui:
# MAGIC
# MAGIC 1. **Dedup**: linhas 100% idênticas são removidas. O `ROW_NUMBER` conserva a primeira por `_ingerido_em`.
# MAGIC 2. **Enriquecimento**: JOIN com `silver.empresas` (nome da companhia), `silver.codigos_operacao` (descrições de DI e tipo de linha).
# MAGIC 3. **Regras de negócio**: escopo doméstico/internacional, dia da semana, mês de referência, faixa plausível de atraso, pontualidade a 15 minutos.
# MAGIC
# MAGIC Atraso fora de faixa (< -2h ou > 24h) anula as três métricas de atraso e marca `atraso_fora_de_faixa = true`. A linha continua no fato — não é exclusão, é nulificação.

# COMMAND ----------

# DBTITLE 1,Criar fato_voos
spark.sql("""
CREATE OR REPLACE TABLE voebem.gold.fato_voos AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        icao_empresa, numero_voo, codigo_di, codigo_tipo_linha,
        icao_origem, icao_destino,
        partida_prevista, partida_real,
        chegada_prevista, chegada_real,
        situacao_voo, codigo_justificativa,
        atraso_partida_min, atraso_chegada_min, minutos_recuperados,
        _arquivo_origem, _ingerido_em, _transformado_em
      ORDER BY _ingerido_em
    ) AS rn
  FROM voebem.silver.vra
),
base AS (
  SELECT
    icao_empresa, numero_voo, codigo_di, codigo_tipo_linha,
    icao_origem, icao_destino,
    partida_prevista, partida_prevista_data, partida_prevista_hora,
    partida_real, chegada_prevista, chegada_real,
    situacao_voo,
    atraso_partida_min, atraso_chegada_min, minutos_recuperados
  FROM dedup
  WHERE rn = 1
),
enriched AS (
  SELECT
    b.*,

    -- companhia
    COALESCE(e.razao_social, 'COMPANHIA NAO CADASTRADA ' || b.icao_empresa) AS nome_companhia,
    e.origem_cadastro AS cadastro_companhia,

    -- descricoes de operacao
    COALESCE(di.descricao, 'Codigo nao catalogado pela ANAC') AS descricao_di,
    COALESCE(tl.descricao, 'Codigo nao catalogado pela ANAC') AS descricao_tipo_linha,

    -- escopo: N/C = Domestico, I/G = Internacional
    CASE
      WHEN b.codigo_tipo_linha IN ('N', 'C') THEN 'Domestico'
      WHEN b.codigo_tipo_linha IN ('I', 'G') THEN 'Internacional'
    END AS escopo_voo,

    -- hora cheia da partida prevista (0-23)
    HOUR(b.partida_prevista) AS hora_partida_prevista,

    -- dia da semana por extenso e em minusculas
    CASE dayofweek(b.partida_prevista_data)
      WHEN 1 THEN 'domingo'
      WHEN 2 THEN 'segunda-feira'
      WHEN 3 THEN 'terca-feira'
      WHEN 4 THEN 'quarta-feira'
      WHEN 5 THEN 'quinta-feira'
      WHEN 6 THEN 'sexta-feira'
      WHEN 7 THEN 'sabado'
    END AS dia_semana,

    -- mes de referencia (primeiro dia do mes)
    date_trunc('MM', b.partida_prevista_data) AS mes_referencia,

    -- atraso fora de faixa: < -120 min ou > 1440 min
    CASE
      WHEN (b.atraso_partida_min IS NOT NULL
            AND (b.atraso_partida_min < -120 OR b.atraso_partida_min > 1440))
        OR (b.atraso_chegada_min IS NOT NULL
            AND (b.atraso_chegada_min < -120 OR b.atraso_chegada_min > 1440))
      THEN true
      ELSE false
    END AS atraso_fora_de_faixa

  FROM base b
  LEFT JOIN voebem.silver.empresas e       ON b.icao_empresa = e.icao
  LEFT JOIN voebem.silver.codigos_operacao di ON di.dominio = 'codigo_di'        AND di.codigo = b.codigo_di
  LEFT JOIN voebem.silver.codigos_operacao tl ON tl.dominio = 'codigo_tipo_linha' AND tl.codigo = b.codigo_tipo_linha
),
final AS (
  SELECT
    icao_empresa, nome_companhia, numero_voo,
    codigo_di, descricao_di, codigo_tipo_linha, descricao_tipo_linha, escopo_voo,
    icao_origem, icao_destino,
    icao_origem || ' - ' || icao_destino AS rota,
    cadastro_companhia,
    partida_prevista, partida_prevista_data, partida_prevista_hora,
    hora_partida_prevista, dia_semana, mes_referencia,
    partida_real, chegada_prevista, chegada_real,

    -- atraso anulado quando fora de faixa
    CASE WHEN atraso_fora_de_faixa THEN NULL ELSE atraso_partida_min  END AS atraso_partida_min,
    CASE WHEN atraso_fora_de_faixa THEN NULL ELSE atraso_chegada_min  END AS atraso_chegada_min,
    CASE WHEN atraso_fora_de_faixa THEN NULL ELSE minutos_recuperados END AS minutos_recuperados,
    atraso_fora_de_faixa,

    situacao_voo
  FROM enriched
)
SELECT
  icao_empresa, nome_companhia, numero_voo,
  codigo_di, descricao_di, codigo_tipo_linha, descricao_tipo_linha, escopo_voo,
  icao_origem, icao_destino, rota, cadastro_companhia,
  partida_prevista, partida_prevista_data, partida_prevista_hora,
  hora_partida_prevista, dia_semana, mes_referencia,
  partida_real, chegada_prevista, chegada_real,
  atraso_partida_min, atraso_chegada_min, minutos_recuperados, atraso_fora_de_faixa,

  -- pontualidade: 15 minutos. NULL quando nao da para avaliar.
  CASE
    WHEN atraso_partida_min IS NULL THEN NULL
    WHEN atraso_partida_min <= 15 THEN true
    ELSE false
  END AS partida_pontual,
  CASE
    WHEN atraso_chegada_min IS NULL THEN NULL
    WHEN atraso_chegada_min <= 15 THEN true
    ELSE false
  END AS chegada_pontual,

  situacao_voo,
  situacao_voo = 'REALIZADO'  AS voo_realizado,
  situacao_voo = 'CANCELADO'  AS voo_cancelado,
  current_timestamp() AS _processado_em
FROM final
""")

print("gold.fato_voos criada")

# COMMAND ----------

# DBTITLE 1,Verificar fato_voos
display(spark.sql("""
    SELECT
      (SELECT COUNT(*) FROM voebem.silver.vra)         AS silver_vra,
      (SELECT COUNT(*) FROM voebem.gold.fato_voos)       AS gold_fato,
      (SELECT COUNT(*) FROM voebem.silver.vra)
        - (SELECT COUNT(*) FROM voebem.gold.fato_voos)     AS duplicatas_removidas,
      SUM(CASE WHEN situacao_voo = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados,
      SUM(CASE WHEN mes_referencia IS NULL THEN 1 ELSE 0 END)   AS mes_ref_null,
      SUM(CASE WHEN atraso_fora_de_faixa THEN 1 ELSE 0 END)      AS atraso_fora_faixa
    FROM voebem.gold.fato_voos
"""))

# COMMAND ----------

# DBTITLE 1,dim_aeroporto — dimensão de aeroporto
# MAGIC %md
# MAGIC ## 2. `dim_aeroporto` — dimensão de aeroporto
# MAGIC
# MAGIC Coleta todos os códigos ICAO presentes no fato (origem e destino) e enriquece com o cadastro da ANAC. Aeroporto estrangeiro não está no cadastro brasileiro — aparece com nome de fallback e `no_cadastro_anac = false`.

# COMMAND ----------

# DBTITLE 1,Criar dim_aeroporto
spark.sql("""
CREATE OR REPLACE TABLE voebem.gold.dim_aeroporto AS
WITH aeroportos_fato AS (
  SELECT icao_origem  AS icao FROM voebem.gold.fato_voos
  UNION
  SELECT icao_destino AS icao FROM voebem.gold.fato_voos
)
SELECT
  a.icao AS icao_aeroporto,
  COALESCE(ae.nome, 'AEROPORTO FORA DO CADASTRO ANAC ' || a.icao) AS nome_aeroporto,
  ae.municipio AS municipio_aeroporto,
  ae.uf_nome   AS uf_aeroporto,
  CASE WHEN a.icao LIKE 'S%' THEN 'Brasil' ELSE 'Exterior' END AS pais_aeroporto,
  ae.icao IS NOT NULL AS no_cadastro_anac,
  current_timestamp() AS _processado_em
FROM aeroportos_fato a
LEFT JOIN voebem.silver.aerodromos ae ON a.icao = ae.icao
""")

print("gold.dim_aeroporto criada")

# COMMAND ----------

# DBTITLE 1,obt_voos — One Big Table
# MAGIC %md
# MAGIC ## 3. `obt_voos` — One Big Table desnormalizada
# MAGIC
# MAGIC A OBT é o fato com a dimensão aeroporto JOINada duas vezes (origem e destino). Nenhum JOIN sobra para o consumidor — é uma tabela, uma linha por etapa, responde sem montar nada.

# COMMAND ----------

# DBTITLE 1,Criar obt_voos
spark.sql("""
CREATE OR REPLACE TABLE voebem.gold.obt_voos AS
SELECT
  f.icao_empresa, f.nome_companhia, f.numero_voo,
  f.codigo_di, f.descricao_di, f.codigo_tipo_linha, f.descricao_tipo_linha, f.escopo_voo,

  -- origem
  f.icao_origem,
  COALESCE(o.nome_aeroporto, 'AEROPORTO FORA DO CADASTRO ANAC ' || f.icao_origem) AS nome_aeroporto_origem,
  o.municipio_aeroporto AS municipio_origem,
  o.uf_aeroporto        AS uf_origem,
  o.pais_aeroporto      AS pais_origem,

  -- destino
  f.icao_destino,
  COALESCE(d.nome_aeroporto, 'AEROPORTO FORA DO CADASTRO ANAC ' || f.icao_destino) AS nome_aeroporto_destino,
  d.municipio_aeroporto AS municipio_destino,
  d.uf_aeroporto        AS uf_destino,
  d.pais_aeroporto      AS pais_destino,

  -- rota
  f.rota AS rota_icao,
  COALESCE(o.municipio_aeroporto, f.icao_origem) || ' - '
    || COALESCE(d.municipio_aeroporto, f.icao_destino) AS rota_municipios,

  -- tempo
  f.partida_prevista, f.partida_prevista_data, f.partida_prevista_hora,
  f.hora_partida_prevista, f.dia_semana, f.mes_referencia,
  f.partida_real, f.chegada_prevista, f.chegada_real,

  -- metricas
  f.atraso_partida_min, f.atraso_chegada_min, f.minutos_recuperados, f.atraso_fora_de_faixa,
  f.partida_pontual, f.chegada_pontual,

  -- situacao
  f.situacao_voo, f.voo_realizado, f.voo_cancelado,
  f._processado_em
FROM voebem.gold.fato_voos f
LEFT JOIN voebem.gold.dim_aeroporto o ON f.icao_origem  = o.icao_aeroporto
LEFT JOIN voebem.gold.dim_aeroporto d ON f.icao_destino = d.icao_aeroporto
""")

print("gold.obt_voos criada")

# COMMAND ----------

# DBTITLE 1,Verificar obt_voos
display(spark.sql("""
    SELECT
      COUNT(*)                                                       AS recuperou_algum_minuto,
      SUM(CASE WHEN atraso_chegada_min <= 0 THEN 1 ELSE 0 END)      AS chegou_adiantado_ou_no_horario,
      SUM(CASE WHEN atraso_chegada_min >  0 THEN 1 ELSE 0 END)      AS chegou_atrasado_mesmo_assim,
      SUM(CASE WHEN atraso_chegada_min > 15 THEN 1 ELSE 0 END)      AS chegou_atrasado_mais_de_15
    FROM voebem.gold.obt_voos
    WHERE minutos_recuperados > 0
"""))

# COMMAND ----------

# DBTITLE 1,Resumo das tres tabelas
display(spark.sql("""
    SELECT 'fato_voos'      AS tabela, COUNT(*) AS linhas FROM voebem.gold.fato_voos
    UNION ALL
    SELECT 'dim_aeroporto',        COUNT(*)         FROM voebem.gold.dim_aeroporto
    UNION ALL
    SELECT 'obt_voos',             COUNT(*)         FROM voebem.gold.obt_voos
"""))