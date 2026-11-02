-- =============================================================================
-- Tech Challenge Fase 3 | Consultas analíticas no Amazon Athena
-- Bancos (Glue Data Catalog): sod_bronze, sod_silver, sod_gold
-- Antes de rodar: Athena > Settings > Query result location = s3://lab-<id>/athena-results/
-- =============================================================================

-- 0) Conferência da ingestão: linhas por edição na Silver
--    (se a tabela vier vazia, rode antes: MSCK REPAIR TABLE sod_silver.respondentes;)
SELECT edicao, COUNT(*) AS respondentes
FROM sod_silver.respondentes
GROUP BY edicao
ORDER BY edicao;

-- 1) Painel executivo: KPIs por edição
SELECT edicao, respondentes, pct_feminino, pct_remoto, pct_usa_ia, pct_empresa_paga_ia,
       pct_setor_financeiro, faixa_mediana, pct_acima_16k
FROM sod_gold.gold_resumo_edicao
ORDER BY edicao;

-- 2) Como está estruturado o mercado? Cargos e níveis (% dos respondentes)
SELECT edicao, dimensao, categoria, respondentes, pct
FROM sod_gold.gold_perfil_mercado
WHERE dimensao IN ('cargo', 'nivel', 'setor', 'vinculo')
ORDER BY dimensao, edicao, pct DESC;

-- 3) Quais perfis são mais valorizados? Cargo x faixa mediana x % acima de R$ 16 mil
SELECT categoria AS cargo, faixa_mediana, pct_acima_16k, respondentes
FROM sod_gold.gold_salario
WHERE dimensao = 'cargo' AND edicao = '2025_2026'
ORDER BY faixa_mediana_ordem DESC, pct_acima_16k DESC;

-- 3b) Evolução da remuneração do Sênior comparável (Sênior + Especialista/Staff+)
SELECT edicao, faixa_mediana, pct_acima_16k, respondentes
FROM sod_gold.gold_salario
WHERE dimensao = 'nivel_comparavel' AND categoria = 'Sênior'
ORDER BY edicao;

-- 4) Diversidade de gênero: % de mulheres por nível e lacuna salarial no Sênior
SELECT edicao, dimensao, categoria, respondentes, pct_feminino
FROM sod_gold.gold_diversidade_genero
WHERE dimensao IN ('total', 'nivel_comparavel')
ORDER BY dimensao, categoria, edicao;

SELECT edicao, categoria, faixa_mediana, pct_acima_16k
FROM sod_gold.gold_salario
WHERE dimensao = 'genero_nivel' AND categoria LIKE 'Sênior |%'
ORDER BY edicao, categoria;

-- 5) Tecnologias com maior adoção em 2025-26 (top 5 por grupo)
SELECT grupo, tecnologia, pct
FROM (
    SELECT grupo, tecnologia, pct,
           ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY pct DESC) AS posicao
    FROM sod_gold.gold_tecnologias
    WHERE edicao = '2025_2026' AND tecnologia NOT IN ('nenhuma')
) t
WHERE posicao <= 5
ORDER BY grupo, pct DESC;

-- 5b) Cloud: evolução de AWS, Azure e GCP (só tecnologias presentes nas 3 edições)
SELECT tecnologia, edicao, pct
FROM sod_gold.gold_tecnologias
WHERE grupo = 'Cloud' AND tecnologia IN ('aws', 'azure', 'gcp') AND presente_nas_3_edicoes = 1
ORDER BY tecnologia, edicao;

-- 6) Adoção de IA generativa: uso individual e ferramenta paga pela empresa
SELECT edicao, dimensao, categoria, base, pct_usa_ia, pct_empresa_paga
FROM sod_gold.gold_ia_adocao
WHERE dimensao IN ('total', 'segmento_setor')
ORDER BY dimensao, categoria, edicao;

-- 6b) Impacto: remuneração por perfil de uso de IA
SELECT edicao, categoria AS perfil_uso_ia, faixa_mediana, pct_acima_16k, respondentes
FROM sod_gold.gold_salario
WHERE dimensao = 'perfil_uso_ia'
ORDER BY edicao, pct_acima_16k DESC;

-- 6c) Visão do gestor: prioridade e barreiras para IA na empresa
SELECT edicao, indicador, categoria, pct
FROM sod_gold.gold_ia_empresa
WHERE indicador IN ('prioridade', 'barreira')
ORDER BY indicador, edicao, pct DESC;

-- 7) Diferenças por região e modelo de trabalho
SELECT dimensao, categoria, edicao, faixa_mediana, pct_acima_16k, respondentes
FROM sod_gold.gold_salario
WHERE dimensao IN ('regiao', 'modelo_trabalho') AND edicao = '2025_2026'
ORDER BY dimensao, pct_acima_16k DESC;

-- 7b) Modelo de trabalho atual x ideal (gap de expectativa)
SELECT edicao, dimensao, categoria, pct
FROM sod_gold.gold_perfil_mercado
WHERE dimensao IN ('modelo_trabalho', 'modelo_ideal')
ORDER BY edicao, categoria, dimensao;

-- 8) Consulta ad hoc direto na Silver: setor financeiro x demais (2025-26)
SELECT CASE WHEN setor_financeiro = 1 THEN 'Finanças/Bancos' ELSE 'Demais setores' END AS segmento,
       COUNT(*) AS respondentes,
       ROUND(AVG(CASE WHEN genero = 'Feminino' THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_feminino,
       ROUND(AVG(CASE WHEN modelo_trabalho = 'Remoto' THEN 1.0 ELSE 0.0 END) * 100, 1) AS pct_remoto,
       ROUND(AVG(CAST(ia_uso__empresa_paga AS DOUBLE)) * 100, 1) AS pct_empresa_paga_ia
FROM sod_silver.respondentes
WHERE edicao = '2025_2026' AND setor IS NOT NULL
GROUP BY 1
ORDER BY 1;
