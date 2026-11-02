"""
Tech Challenge Fase 3 | Job 02 - Bronze -> Silver (harmonização das 3 edições)

A Data Hackers muda o código e o nome das perguntas a cada edição
(ex.: modelo de trabalho = P2_r em 2023, 2.r em 2024-25 e 2.q em 2025-26).
Este job aplica o DE-PARA de colunas, padroniza valores e une as 3 edições numa
única tabela analítica (sod_silver.respondentes), particionada por edição.

Regras de tratamento (validadas na etapa de de-para):
  - 'Especialista/Staff+' (só existe em 2025-26) é mantido em `nivel`;
    em `nivel_comparavel` ele é somado a 'Sênior' para comparações entre anos.
  - Linguagens 2025-26: o bloco 4.c (rotulado "linguagem_preferida") é de
    múltipla escolha e se comporta como "uso no dia a dia" -> tratado assim;
    comparações entre anos usam só as linguagens presentes nas 3 listas.
  - Salário: só existe em faixas. Mantemos a faixa + ordem (1..13); a Gold usa
    faixa mediana e distribuição por faixa (nenhum valor é inventado).
  - Correção de erros de digitação da própria base (faixas e porte).
  - Flags multi-valoradas viram colunas 0/1 com prefixo do bloco:
    lang__, db__, cloud__, bi__, ia_uso__, ia_barreira__, ia_empresa__.
"""
import os
import re
import sys
from functools import reduce

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

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
    spark = (SparkSession.builder.appName("job_02_silver").master("local[*]")
             .config("spark.sql.warehouse.dir", os.path.join(ROOT, "data_lake", "_warehouse"))
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    BASE = os.path.join(ROOT, "data_lake")

DB_SILVER = "sod_silver"


def caminho(*partes):
    return "/".join([BASE, *partes]) if IS_GLUE else os.path.join(BASE, *partes)


# ---------------------------------------------------------------------------
# 1) DE-PARA DE COLUNAS (prefixo da pergunta em cada edição, nome sanitizado)
# ---------------------------------------------------------------------------
EDICOES = ["2023", "2024_2025", "2025_2026"]
ANO_REFERENCIA = {"2023": 2023, "2024_2025": 2024, "2025_2026": 2025}  # ano da coleta

DE_PARA = {
    #  coluna harmonizada     2023          2024-2025     2025-2026
    "id_respondente":       ("p0_",        "q0_a_",      "q0_a_"),
    "idade":                ("p1_a_",      "q1_a_",      "q1_a_"),
    "faixa_idade":          ("p1_a_1_",    "q1_a_1_",    "q1_a_1_"),
    "genero":               ("p1_b_",      "q1_b_",      "q1_b_"),
    "cor_raca_etnia":       ("p1_c_",      "q1_c_",      "q1_c_"),
    "pcd":                  ("p1_d_",      "q1_d_",      "q1_d_"),
    "uf":                   ("p1_i_1_",    "q1_i_1_",    "q1_i_1_"),
    "regiao":               ("p1_i_2_",    "q1_i_2_",    "q1_i_2_"),
    "nivel_ensino":         ("p1_l_",      "q1_l_",      "q1_l_"),
    "area_formacao":        ("p1_m_",      "q1_m_",      "q1_m_"),
    "situacao_trabalho":    ("p2_a_",      "q2_a_",      "q2_a_"),
    "setor":                ("p2_b_",      "q2_b_",      "q2_b_"),
    "porte_empresa":        ("p2_c_",      "q2_c_",      "q2_c_"),
    "gestor_raw":           ("p2_d_",      "q2_d_",      "q2_d_"),
    "cargo_gestor":         ("p2_e_",      "q2_e_",      "q2_e_"),
    "cargo_raw":            ("p2_f_",      "q2_f_",      "q2_f_"),
    "nivel_raw":            ("p2_g_",      "q2_g_",      "q2_g_"),
    "faixa_salarial_raw":   ("p2_h_",      "q2_h_",      "q2_h_"),
    "tempo_exp_dados_raw":  ("p2_i_",      "q2_i_",      "q2_i_"),
    "modelo_trabalho_raw":  ("p2_r_",      "q2_r_",      "q2_q_"),
    "modelo_ideal_raw":     ("p2_s_",      "q2_s_",      "q2_r_"),
    "ia_prioridade_raw":    ("p3_e_",      "q3_e_",      "q3_e_"),
    "cloud_preferida":      ("p4_i_",      "q4_i_",      "q4_f_"),
    "bi_preferida":         ("p4_k_",      "q4_k_",      "q4_h_"),
}

# Blocos de múltipla escolha (cada opção = 1 coluna 0/1 na origem)
BLOCOS = {
    #  prefixo harmonizado   2023       2024-2025   2025-2026
    "lang":                ("p4_d_",   "q4_d_",    "q4_c_"),
    "db":                  ("p4_g_",   "q4_g_",    "q4_d_"),
    "cloud":               ("p4_h_",   "q4_h_",    "q4_e_"),
    "bi":                  ("p4_j_",   "q4_j_",    "q4_g_"),
    "ia_uso":              ("p4_m_",   "q4_m_",    "q4_j_"),
    "ia_barreira":         ("p3_g_",   "q3_g_",    "q3_h_"),
    "ia_empresa":          ("p3_f_",   "q3_f_",    "q3_f_"),
}

# Nome curto e estável para opções cujo rótulo muda entre edições
OPCAO_CANONICA = {
    "r": "r", "c_c_c": "c_cpp_csharp", "visual_basic_vba": "vba", "sas_stata": "sas_stata",
    "nao_utilizo_nenhuma_linguagem": "nenhuma",
    "nao_utilizo_nenhuma_das_linguagens_listadas": "nenhuma",
    "azure_microsoft": "azure", "amazon_web_services_aws": "aws", "google_cloud_gcp": "gcp",
    "oracle_cloud": "oracle", "servidores_on_premise_nao_utilizamos_cloud": "on_premise",
    "microsoft_powerbi": "power_bi", "qlik_view_qlik_sense": "qlik",
    "looker_studio_google_data_studio": "looker_studio", "amazon_quicksight": "quicksight",
    "fazemos_todas_as_analises_utilizando_apenas_excel_ou_planilhas_do_google": "excel_planilhas",
    "nao_utilizo_nenhuma_ferramenta_de_bi_no_trabalho": "nenhuma",
    "nao_uso_solucoes_de_ai_generativa_com_foco_em_produtividade": "nao_usa",
    "uso_solucoes_gratuitas_de_ai_generativa_com_foco_em_produtividade": "gratuitas",
    "uso_e_pago_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade": "paga_do_proprio_bolso",
    "a_empresa_que_trabalho_paga_pelas_solucoes_de_ai_generativa_com_foco_em_produtividade": "empresa_paga",
    "uso_solucoes_do_tipo_copilot": "copilot",
    "falta_de_compreensao_dos_casos_de_uso": "casos_de_uso",
    "falta_de_confiabilidade_das_saidas_alucinacao_dos_modelos": "alucinacao",
    "incerteza_em_relacao_a_regulamentacao": "regulamentacao",
    "preocupacoes_com_seguranca_e_privacidade_de_dados": "seguranca_privacidade",
    "retorno_sobre_investimento_roi_nao_comprovado_de_ia_generativa": "roi_nao_comprovado",
    "dados_da_empresa_nao_estao_prontos_para_uso_de_ia_generativa": "dados_nao_prontos",
    "falta_de_expertise_ou_falta_de_recursos": "falta_expertise",
    "alta_direcao_da_empresa_nao_ve_valor_ou_nao_ve_como_prioridade": "alta_direcao",
    "preocupacoes_com_propriedade_intelectual": "propriedade_intelectual",
    "colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada": "descentralizado",
    "direcionamento_centralizado_do_uso_de_ai_generativa": "centralizado",
    "desenvolvedores_utilizando_copilots": "devs_copilot",
    "ai_generativa_e_llms_para_melhorar_produtos_externos": "produtos_externos",
    "ai_generativa_e_llms_para_melhorar_produtos_externos_para_os_clientes_finais": "produtos_externos",
    "ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores": "produtos_internos",
    "ia_generativa_e_llms_como_principal_frente_do_negocio": "frente_principal",
    "ia_generativa_e_llms_nao_e_prioridade": "nao_prioridade",
    "nao_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa": "nao_sei",
}


def achar_coluna(colunas, prefixo):
    """Coluna simples = prefixo seguido de letra (ex.: 'p2_h_faixa_salarial')."""
    padrao = re.compile("^" + re.escape(prefixo) + "[a-z]")
    achadas = [c for c in colunas if padrao.match(c)]
    if not achadas:
        raise ValueError(f"Coluna com prefixo '{prefixo}' não encontrada")
    return achadas[0]


def colunas_do_bloco(colunas, prefixo):
    """Opções de um bloco = prefixo + número + rótulo (ex.: 'q4_d_3_python')."""
    saida = {}
    for c in colunas:
        m = re.match("^" + re.escape(prefixo) + r"\d+_(.+)$", c)
        if m:
            rotulo = m.group(1)
            saida[OPCAO_CANONICA.get(rotulo, rotulo)] = c
    return saida


def flag(col):
    """'1.0' / 'True' -> 1 ; '0.0' / 'False' -> 0 ; vazio -> null"""
    v = F.lower(F.trim(F.col(col)))
    return (F.when(v.isin("1", "1.0", "true"), 1)
             .when(v.isin("0", "0.0", "false"), 0)
             .otherwise(F.lit(None)).cast(T.IntegerType()))


def harmonizar_edicao(edicao: str) -> DataFrame:
    i = EDICOES.index(edicao)
    bronze = spark.read.parquet(caminho("bronze", f"state_of_data_{edicao}"))
    cols = bronze.columns

    selecao = [F.col(achar_coluna(cols, pref[i])).alias(nome) for nome, pref in DE_PARA.items()]
    for bloco, pref in BLOCOS.items():
        for opcao, col in colunas_do_bloco(cols, pref[i]).items():
            selecao.append(flag(col).alias(f"{bloco}__{opcao}"))

    return (bronze.select(*selecao)
            .withColumn("edicao", F.lit(edicao))
            .withColumn("ano_coleta", F.lit(ANO_REFERENCIA[edicao])))


# ---------------------------------------------------------------------------
# 2) UNIÃO DAS 3 EDIÇÕES (opção ausente num ano vira null, não zero)
# ---------------------------------------------------------------------------
df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
            [harmonizar_edicao(e) for e in EDICOES])

# ---------------------------------------------------------------------------
# 3) PADRONIZAÇÃO DE VALORES
# ---------------------------------------------------------------------------
FAIXAS = [  # (texto original, rótulo curto, ordem)
    ("Menos de R$ 1.000/mês", "Até 1k", 1),
    ("de R$ 1.001/mês a R$ 2.000/mês", "1-2k", 2),
    ("de R$ 2.001/mês a R$ 3.000/mês", "2-3k", 3),
    ("de R$ 3.001/mês a R$ 4.000/mês", "3-4k", 4),
    ("de R$ 4.001/mês a R$ 6.000/mês", "4-6k", 5),
    ("de R$ 6.001/mês a R$ 8.000/mês", "6-8k", 6),
    ("de R$ 8.001/mês a R$ 12.000/mês", "8-12k", 7),
    ("de R$ 12.001/mês a R$ 16.000/mês", "12-16k", 8),
    ("de R$ 16.001/mês a R$ 20.000/mês", "16-20k", 9),
    ("de R$ 20.001/mês a R$ 25.000/mês", "20-25k", 10),
    ("de R$ 25.001/mês a R$ 30.000/mês", "25-30k", 11),
    ("de R$ 30.001/mês a R$ 40.000/mês", "30-40k", 12),
    ("Acima de R$ 40.001/mês", "Acima de 40k", 13),
]
CORRECOES_FAIXA = {  # erros de digitação presentes na própria base
    "de R$ 101/mês a R$ 2.000/mês": "de R$ 1.001/mês a R$ 2.000/mês",
    "de R$ 25.001/mês a R$ 3000/mês": "de R$ 25.001/mês a R$ 30.000/mês",
}

CARGOS = {
    "Analista de Dados/Data Analyst": "Analista de Dados",
    "Cientista de Dados/Data Scientist": "Cientista de Dados",
    "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect": "Engenheiro/Arquiteto de Dados",
    "Engenheiro de Dados/Data Engineer/Data Architect": "Engenheiro/Arquiteto de Dados",
    "Arquiteto de Dados/Data Architect": "Engenheiro/Arquiteto de Dados",
    "Analista de BI/BI Analyst": "Analista de BI",
    "Analista de Negócios/Business Analyst": "Analista de Negócios",
    "Analytics Engineer": "Analytics Engineer",
    "Engenheiro de Machine Learning/ML Engineer/AI Engineer": "Engenheiro de ML/IA",
    "Desenvolvedor/ Engenheiro de Software/ Analista de Sistemas": "Dev/Eng. de Software",
    "Data Product Manager/ Product Manager (PM/APM/DPM/GPM/PO)": "Product Manager",
}

MODELOS = {
    "Modelo 100% remoto": "Remoto",
    "Modelo híbrido flexível (o funcionário tem liberdade para escolher quando estar no escritório presencialmente)": "Híbrido flexível",
    "Modelo híbrido com dias fixos de trabalho presencial": "Híbrido fixo",
    "Modelo 100% presencial": "Presencial",
}

TEMPO_EXP = {  # 2023 tinha as opções sobrepostas "4 a 6" e "5 a 6"
    "Não tenho experiência na área de dados": ("Sem experiência", 0),
    "Menos de 1 ano": ("Menos de 1 ano", 1),
    "de 1 a 2 anos": ("1 a 2 anos", 2),
    "de 3 a 4 anos": ("3 a 4 anos", 3),
    "de 4 a 6 anos": ("5 a 6 anos", 4),
    "de 5 a 6 anos": ("5 a 6 anos", 4),
    "de 7 a 10 anos": ("7 a 10 anos", 5),
    "Mais de 10 anos": ("Mais de 10 anos", 6),
}


def mapear(coluna, dicionario, padrao=None):
    expr = None
    for origem, destino in dicionario.items():
        cond = F.col(coluna) == origem
        expr = F.when(cond, destino) if expr is None else expr.when(cond, destino)
    return expr.otherwise(padrao if padrao is not None else F.lit(None))


faixa_corrigida = mapear("faixa_salarial_raw", CORRECOES_FAIXA, F.col("faixa_salarial_raw"))
df = df.withColumn("faixa_salarial_original", faixa_corrigida)
df = (df.withColumn("faixa_salarial", mapear("faixa_salarial_original", {a: b for a, b, _ in FAIXAS}))
        .withColumn("faixa_salarial_ordem",
                    mapear("faixa_salarial_original", {a: o for a, _, o in FAIXAS}).cast("int")))

gestor = F.lower(F.col("gestor_raw")).isin("1", "1.0", "true")
df = (df.withColumn("gestor", F.when(F.col("gestor_raw").isNull(), None).otherwise(gestor.cast("int")))
        .withColumn("nivel", F.when(F.col("gestor") == 1, "Gestor").otherwise(F.col("nivel_raw")))
        .withColumn("nivel_comparavel",
                    F.when(F.col("nivel") == "Especialista/Staff+", "Sênior").otherwise(F.col("nivel")))
        .withColumn("cargo",
                    F.when(F.col("gestor") == 1, "Gestor")
                     .when(F.col("cargo_raw").isNull(), None)
                     .otherwise(mapear("cargo_raw", CARGOS, F.lit("Outros")))))

df = (df.withColumn("modelo_trabalho", mapear("modelo_trabalho_raw", MODELOS))
        .withColumn("modelo_ideal", mapear("modelo_ideal_raw", MODELOS))
        .withColumn("tempo_exp_dados", mapear("tempo_exp_dados_raw", {k: v[0] for k, v in TEMPO_EXP.items()}))
        .withColumn("tempo_exp_dados_ordem",
                    mapear("tempo_exp_dados_raw", {k: v[1] for k, v in TEMPO_EXP.items()}).cast("int"))
        .withColumn("porte_empresa",
                    F.when(F.col("porte_empresa") == "de 501 a 100", "de 501 a 1.000")
                     .otherwise(F.col("porte_empresa")))
        .withColumn("idade", F.col("idade").cast("double").cast("int")))

situacao = F.col("situacao_trabalho")
df = df.withColumn(
    "vinculo",
    F.when(situacao == "Empregado (CLT)", "CLT")
     .when(situacao == "Empreendedor ou Empregado (CNPJ)", "PJ/Empreendedor")
     .when(situacao == "Servidor Público", "Servidor Público")
     .when(situacao == "Estagiário", "Estágio")
     .when(situacao.contains("empresa de fora do Brasil"), "Empresa estrangeira")
     .when(situacao == "Freelancer", "Freelancer")
     .when(situacao.startswith("Desempregado"), "Desempregado")
     .when(situacao.startswith("Somente Estudante") | situacao.contains("Acadêmica"), "Estudante/Acadêmico")
     .otherwise("Não informado"))

prioridade = F.col("ia_prioridade_raw")
df = df.withColumn(
    "ia_prioridade_empresa",
    F.when(prioridade.startswith("Sim, é nossa principal"), "Principal prioridade")
     .when(prioridade.startswith("Sim, está entre"), "Entre as prioridades (2-4 anos)")
     .when(prioridade.startswith("Mais ou menos"), "Iniciativas isoladas")
     .when(prioridade.startswith("Não é uma iniciativa"), "Não é prioridade")
     .when(prioridade.startswith("Não sei"), "Não sei opinar"))

# Indicadores derivados
df = (df.withColumn("setor_financeiro", (F.col("setor") == "Finanças ou Bancos").cast("int"))
        .withColumn("mora_no_brasil", F.col("regiao").isNotNull().cast("int"))
        .withColumn("ia_usa_no_trabalho",           # respondeu o bloco e não marcou "não uso"
                    F.when(F.col("ia_uso__nao_usa").isNull(), None)
                     .otherwise(1 - F.col("ia_uso__nao_usa"))))

df = (df.drop("gestor_raw", "nivel_raw", "cargo_raw", "faixa_salarial_raw", "tempo_exp_dados_raw",
              "modelo_trabalho_raw", "modelo_ideal_raw", "ia_prioridade_raw")
        .dropDuplicates(["edicao", "id_respondente"]))

# ---------------------------------------------------------------------------
# 4) GRAVAÇÃO (Parquet particionado por edição + Data Catalog)
# ---------------------------------------------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_SILVER}")
(df.write.mode("overwrite").format("parquet")
   .option("compression", "snappy")
   .option("path", caminho("silver", "respondentes"))
   .partitionBy("edicao")
   .saveAsTable(f"{DB_SILVER}.respondentes"))

resumo = df.groupBy("edicao").count().orderBy("edicao").collect()
print("[SILVER] sod_silver.respondentes:", {r["edicao"]: r["count"] for r in resumo},
      f"| {len(df.columns)} colunas")

if IS_GLUE:
    job.commit()
