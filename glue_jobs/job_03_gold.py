"""
Tech Challenge Fase 3 | Job 03 - Silver -> Gold (tabelas analíticas)

Gera as tabelas de consumo que respondem às perguntas de negócio do desafio.
Todas em formato "longo" (edicao, dimensao, categoria, métricas), o que deixa as
consultas no Athena simples e os gráficos fáceis de montar.

  gold_resumo_edicao         KPIs por edição (painel executivo)
  gold_perfil_mercado        distribuição do mercado por dimensão (estrutura do mercado)
  gold_salario               faixa salarial mediana + % acima de R$16k por dimensão
  gold_salario_distribuicao  distribuição completa das faixas salariais por dimensão
  gold_diversidade_genero    % de mulheres por nível, cargo e região
  gold_tecnologias           adoção de linguagens, bancos, cloud e BI
  gold_ia_adocao             uso individual de IA generativa por segmento
  gold_ia_empresa            prioridade, tipos de uso e barreiras de IA (visão do gestor)

Regras de base (denominador) - todo % usa só quem respondeu aquela pergunta:
  - salário: quem informou faixa E mora no Brasil (região preenchida);
  - IA individual: quem respondeu o bloco de uso de IA (não-gestores empregados);
  - IA na empresa: gestores que responderam a pergunta.
"""
import os
import sys
from functools import reduce

from pyspark.sql import SparkSession, Window, functions as F

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    IS_GLUE = True
except ImportError:
    IS_GLUE = False

if IS_GLUE:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    BASE = f"s3://{args['BUCKET']}"
else:
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spark = (SparkSession.builder.appName("job_03_gold").master("local[*]")
             .config("spark.sql.warehouse.dir", os.path.join(ROOT, "data_lake", "_warehouse"))
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.join(ROOT, "data_lake")

DB_GOLD = "sod_gold"


def caminho(*partes):
    return "/".join([BASE, *partes]) if IS_GLUE else os.path.join(BASE, *partes)


s = spark.read.parquet(caminho("silver", "respondentes")).cache()

# Perfil de uso de IA (para medir "impacto": salário x tipo de uso)
s = s.withColumn(
    "perfil_uso_ia",
    F.when(F.col("ia_uso__nao_usa").isNull(), None)
     .when(F.col("ia_uso__empresa_paga") == 1, "Empresa paga a ferramenta")
     .when(F.col("ia_usa_no_trabalho") == 1, "Usa por conta própria")
     .otherwise("Não usa IA"))
s = s.withColumn("segmento_setor",
                 F.when(F.col("setor_financeiro") == 1, "Finanças/Bancos")
                  .when(F.col("setor").isNotNull(), "Demais setores"))
s = s.withColumn("total", F.lit("Total"))
s = s.withColumn("genero_nivel",
                 F.when(F.col("genero").isin("Feminino", "Masculino") & F.col("nivel_comparavel").isNotNull(),
                        F.concat_ws(" | ", "nivel_comparavel", "genero")))

FAIXA_ROTULO = {1: "Até 1k", 2: "1-2k", 3: "2-3k", 4: "3-4k", 5: "4-6k", 6: "6-8k", 7: "8-12k",
                8: "12-16k", 9: "16-20k", 10: "20-25k", 11: "25-30k", 12: "30-40k", 13: "Acima de 40k"}
rotulo_faixa = F.create_map(*[x for k, v in FAIXA_ROTULO.items() for x in (F.lit(k), F.lit(v))])


def empilhar(dfs):
    return reduce(lambda a, b: a.unionByName(b), dfs)


def salvar(df, nome):
    (df.coalesce(1).write.mode("overwrite").format("parquet")
       .option("path", caminho("gold", nome))
       .saveAsTable(f"{DB_GOLD}.{nome}"))
    print(f"[GOLD] {DB_GOLD}.{nome}: {df.count()} linhas")


spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_GOLD}")

# ---------------------------------------------------------------------------
# 1) Estrutura do mercado
# ---------------------------------------------------------------------------
DIM_PERFIL = ["genero", "regiao", "uf", "nivel", "nivel_comparavel", "cargo", "setor", "vinculo",
              "modelo_trabalho", "modelo_ideal", "nivel_ensino", "tempo_exp_dados", "faixa_idade",
              "porte_empresa", "cargo_gestor"]
w_ed = Window.partitionBy("edicao", "dimensao")
perfil = empilhar([
    s.filter(F.col(d).isNotNull())
     .groupBy("edicao", F.lit(d).alias("dimensao"), F.col(d).alias("categoria"))
     .agg(F.count("*").alias("respondentes"))
    for d in DIM_PERFIL
]).withColumn("pct", F.round(F.col("respondentes") / F.sum("respondentes").over(w_ed) * 100, 2))
salvar(perfil, "gold_perfil_mercado")

# ---------------------------------------------------------------------------
# 2) Remuneração (faixa mediana e distribuição)
# ---------------------------------------------------------------------------
DIM_SALARIO = ["total", "nivel", "nivel_comparavel", "cargo", "regiao", "modelo_trabalho", "genero",
               "genero_nivel", "segmento_setor", "vinculo", "tempo_exp_dados", "perfil_uso_ia",
               "nivel_ensino"]
sal = s.filter(F.col("faixa_salarial_ordem").isNotNull() & (F.col("mora_no_brasil") == 1))

dist = empilhar([
    sal.filter(F.col(d).isNotNull())
       .groupBy("edicao", F.lit(d).alias("dimensao"), F.col(d).alias("categoria"),
                "faixa_salarial_ordem")
       .agg(F.count("*").alias("respondentes"))
    for d in DIM_SALARIO
])
w_grp = Window.partitionBy("edicao", "dimensao", "categoria")
w_cum = w_grp.orderBy("faixa_salarial_ordem").rowsBetween(Window.unboundedPreceding, 0)
dist = (dist.withColumn("base", F.sum("respondentes").over(w_grp))
            .withColumn("pct", F.round(F.col("respondentes") / F.col("base") * 100, 2))
            .withColumn("pct_acumulado", F.round(F.sum("respondentes").over(w_cum) / F.col("base") * 100, 2))
            .withColumn("faixa_salarial", rotulo_faixa[F.col("faixa_salarial_ordem")]))
salvar(dist.select("edicao", "dimensao", "categoria", "faixa_salarial", "faixa_salarial_ordem",
                   "respondentes", "base", "pct", "pct_acumulado"), "gold_salario_distribuicao")

# faixa mediana = primeira faixa em que o acumulado atinge 50%
mediana = (dist.filter(F.col("pct_acumulado") >= 50)
               .groupBy("edicao", "dimensao", "categoria")
               .agg(F.min("faixa_salarial_ordem").alias("faixa_mediana_ordem")))
resumo_sal = (dist.groupBy("edicao", "dimensao", "categoria")
                  .agg(F.first("base").alias("respondentes"),
                       F.round(F.sum(F.when(F.col("faixa_salarial_ordem") >= 9, F.col("pct"))), 2)
                        .alias("pct_acima_16k"),
                       F.round(F.sum(F.when(F.col("faixa_salarial_ordem") <= 5, F.col("pct"))), 2)
                        .alias("pct_ate_6k"))
                  .join(mediana, ["edicao", "dimensao", "categoria"])
                  .withColumn("faixa_mediana", rotulo_faixa[F.col("faixa_mediana_ordem")])
                  .fillna(0, ["pct_acima_16k", "pct_ate_6k"]))
salvar(resumo_sal, "gold_salario")

# ---------------------------------------------------------------------------
# 3) Diversidade de gênero
# ---------------------------------------------------------------------------
gen = s.filter(F.col("genero").isNotNull())
div = empilhar([
    gen.filter(F.col(d).isNotNull())
       .groupBy("edicao", F.lit(d).alias("dimensao"), F.col(d).alias("categoria"))
       .agg(F.count("*").alias("respondentes"),
            F.round(F.avg((F.col("genero") == "Feminino").cast("int")) * 100, 2).alias("pct_feminino"))
    for d in ["total", "nivel_comparavel", "nivel", "cargo", "regiao", "segmento_setor", "cargo_gestor"]
])
salvar(div, "gold_diversidade_genero")

# ---------------------------------------------------------------------------
# 4) Tecnologias (linguagens, bancos, cloud, BI)
# ---------------------------------------------------------------------------
GRUPOS = {"lang": "Linguagem", "db": "Banco de dados", "cloud": "Cloud", "bi": "Ferramenta de BI"}


def despivotar(df, colunas, nome_chave="opcao", nome_valor="marcou"):
    """Colunas 0/1 (formato largo) -> linhas (formato longo) com stack(),
    compatível com Spark 3.3 (Glue 4.0) e 3.5 (Glue 5.0)."""
    pares = ", ".join(f"'{c}', `{c}`" for c in colunas)
    return df.select("edicao", F.expr(f"stack({len(colunas)}, {pares}) as ({nome_chave}, {nome_valor})"))


flags_tec = [c for c in s.columns if c.split("__")[0] in GRUPOS]
grupo_map = F.create_map(*[x for k, v in GRUPOS.items() for x in (F.lit(k), F.lit(v))])
tec = (despivotar(s, flags_tec)
       .filter(F.col("marcou").isNotNull())          # null = opção não existia/não respondeu
       .withColumn("grupo", grupo_map[F.split("opcao", "__")[0]])
       .withColumn("tecnologia", F.split("opcao", "__")[1])
       .groupBy("edicao", "grupo", "tecnologia")
       .agg(F.sum("marcou").alias("usuarios"), F.count("*").alias("base_respondentes"))
       .withColumn("pct", F.round(F.col("usuarios") / F.col("base_respondentes") * 100, 2)))
# tecnologia presente na lista das 3 edições? (regra de comparabilidade entre anos)
presenca = tec.groupBy("grupo", "tecnologia").agg(F.countDistinct("edicao").alias("n_edicoes"))
tec = (tec.join(presenca, ["grupo", "tecnologia"])
          .withColumn("presente_nas_3_edicoes", (F.col("n_edicoes") == 3).cast("int"))
          .drop("n_edicoes"))
salvar(tec.select("edicao", "grupo", "tecnologia", "usuarios", "base_respondentes", "pct",
                  "presente_nas_3_edicoes"), "gold_tecnologias")

# ---------------------------------------------------------------------------
# 5) IA generativa - uso individual
# ---------------------------------------------------------------------------
ia = s.filter(F.col("ia_uso__nao_usa").isNotNull())
ia_ad = empilhar([
    ia.filter(F.col(d).isNotNull())
      .groupBy("edicao", F.lit(d).alias("dimensao"), F.col(d).alias("categoria"))
      .agg(F.count("*").alias("base"),
           *[F.round(F.avg(c) * 100, 2).alias(alias) for c, alias in [
               ("ia_usa_no_trabalho", "pct_usa_ia"),
               ("ia_uso__gratuitas", "pct_ferramenta_gratuita"),
               ("ia_uso__paga_do_proprio_bolso", "pct_paga_do_proprio_bolso"),
               ("ia_uso__empresa_paga", "pct_empresa_paga"),
               ("ia_uso__copilot", "pct_copilot")]])
    for d in ["total", "nivel_comparavel", "cargo", "segmento_setor", "regiao", "modelo_trabalho"]
])
salvar(ia_ad, "gold_ia_adocao")

# ---------------------------------------------------------------------------
# 6) IA generativa - visão da empresa (respondida por gestores)
# ---------------------------------------------------------------------------
prior = (s.filter(F.col("ia_prioridade_empresa").isNotNull())
          .groupBy("edicao", F.col("ia_prioridade_empresa").alias("categoria"))
          .agg(F.count("*").alias("respondentes"))
          .withColumn("indicador", F.lit("prioridade"))
          .withColumn("pct", F.round(F.col("respondentes") /
                                     F.sum("respondentes").over(Window.partitionBy("edicao")) * 100, 2)))
flags_emp = [c for c in s.columns if c.startswith(("ia_barreira__", "ia_empresa__"))]
emp = (despivotar(s, flags_emp)
       .filter(F.col("marcou").isNotNull())
       .withColumn("indicador", F.when(F.col("opcao").startswith("ia_barreira"), "barreira")
                                 .otherwise("tipo_de_uso"))
       .withColumn("categoria", F.split("opcao", "__")[1])
       .groupBy("edicao", "indicador", "categoria")
       .agg(F.sum("marcou").alias("respondentes"), F.count("*").alias("base"))
       .withColumn("pct", F.round(F.col("respondentes") / F.col("base") * 100, 2))
       .drop("base"))
salvar(prior.unionByName(emp.select(prior.columns)).select("edicao", "indicador", "categoria", "respondentes", "pct"),
       "gold_ia_empresa")

# ---------------------------------------------------------------------------
# 7) Resumo executivo por edição
# ---------------------------------------------------------------------------
med_total = resumo_sal.filter("dimensao = 'total'").select("edicao", "faixa_mediana", "pct_acima_16k")
kpis = (s.groupBy("edicao")
          .agg(F.count("*").alias("respondentes"),
               F.round(F.avg((F.col("genero") == "Feminino").cast("int")) * 100, 2).alias("pct_feminino"),
               F.round(F.avg(F.when(F.col("modelo_trabalho").isNotNull(),
                                    (F.col("modelo_trabalho") == "Remoto").cast("int"))) * 100, 2)
                .alias("pct_remoto"),
               F.round(F.avg("ia_usa_no_trabalho") * 100, 2).alias("pct_usa_ia"),
               F.round(F.avg("ia_uso__empresa_paga") * 100, 2).alias("pct_empresa_paga_ia"),
               F.round(F.avg(F.when(F.col("setor").isNotNull(), F.col("setor_financeiro"))) * 100, 2)
                .alias("pct_setor_financeiro"),
               F.round(F.avg(F.when(F.col("regiao").isNotNull(),
                                    (F.col("regiao") == "Sudeste").cast("int"))) * 100, 2)
                .alias("pct_sudeste"))
          .join(med_total, "edicao"))
salvar(kpis, "gold_resumo_edicao")

if IS_GLUE:
    job.commit()
