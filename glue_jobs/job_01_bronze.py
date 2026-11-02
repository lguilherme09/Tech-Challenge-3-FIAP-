"""
Tech Challenge Fase 3 | Job 01 - Landing (CSV) -> Bronze (Parquet)

Lê os CSVs brutos das 3 edições do State of Data Brazil, exatamente como baixados
do Kaggle, e grava uma cópia fiel em Parquet na camada Bronze.

Na Bronze NÃO alteramos valores. Fazemos apenas o mínimo técnico necessário:
  - todas as colunas lidas como string (sem inferência de tipo);
  - nomes de colunas sanitizados (Glue Data Catalog / Athena não aceitam
    parênteses, vírgulas, acentos, pontos etc. em nome de coluna);
  - colunas de metadados de linhagem: _edicao, _arquivo_origem, _data_ingestao.

Roda igual em AWS Glue (Job Spark ou Glue Notebook) e localmente (PySpark).
  Glue : parâmetro de job --BUCKET lab-<id-da-conta>
         e "Use Glue Data Catalog as the Hive metastore" habilitado
  Local: python glue_jobs/job_01_bronze.py
"""
import os
import re
import sys
import unicodedata
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------------------------
# Ambiente: AWS Glue ou local
# ---------------------------------------------------------------------------
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
    spark = (SparkSession.builder.appName("job_01_bronze").master("local[*]")
             .config("spark.sql.warehouse.dir", os.path.join(ROOT, "data_lake", "_warehouse"))
             .config("spark.sql.session.timeZone", "America/Sao_Paulo")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.join(ROOT, "data_lake")

DB_BRONZE = "sod_bronze"

# Edição -> arquivo na landing zone
EDICOES = {
    "2023": "state_of_data_2023.csv",
    "2024_2025": "state_of_data_2024_2025.csv",
    "2025_2026": "state_of_data_2025_2026.csv",
}


def landing_path(edicao, arquivo):
    if IS_GLUE:
        return f"{BASE}/landing/state_of_data/edicao={edicao}/{arquivo}"
    return os.path.join(ROOT, "data", "raw", arquivo)


def sanitizar_nome(nome: str) -> str:
    """Converte o cabeçalho original num nome de coluna aceito por Glue/Athena.
    Ex.: "('P2_h ', 'Faixa salarial')" -> "p2_h_faixa_salarial"
         "2.h_faixa_salarial"           -> "q2_h_faixa_salarial"
    """
    s = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()
    if not s or s[0].isdigit():
        s = "q" + s  # Athena exige crase em coluna começando com dígito
    return s[:120]


def nomes_unicos(nomes):
    vistos, saida = set(), []
    for n in nomes:
        base = k = sanitizar_nome(n)
        i = 1
        while k in vistos:
            i += 1
            k = f"{base}_{i}"
        vistos.add(k)
        saida.append(k)
    return saida


spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_BRONZE}")
data_ingestao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for edicao, arquivo in EDICOES.items():
    origem = landing_path(edicao, arquivo)
    df = (spark.read
          .option("header", True)
          .option("multiLine", True)      # há respostas abertas com quebra de linha
          .option("escape", '"')
          .option("encoding", "UTF-8")
          .option("inferSchema", False)   # Bronze = tudo string, fiel à origem
          .csv(origem))

    df = df.toDF(*nomes_unicos(df.columns))
    df = (df.withColumn("_edicao", F.lit(edicao))
            .withColumn("_arquivo_origem", F.lit(arquivo))
            .withColumn("_data_ingestao", F.lit(data_ingestao)))

    tabela = f"state_of_data_{edicao}"
    destino = f"{BASE}/bronze/{tabela}" if IS_GLUE else os.path.join(BASE, "bronze", tabela)

    (df.write.mode("overwrite").format("parquet")
       .option("compression", "snappy")
       .option("path", destino)
       .saveAsTable(f"{DB_BRONZE}.{tabela}"))

    print(f"[BRONZE] {DB_BRONZE}.{tabela}: {df.count()} linhas x {len(df.columns)} colunas")

if IS_GLUE:
    job.commit()
