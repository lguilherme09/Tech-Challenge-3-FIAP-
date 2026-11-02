# Tech Challenge — Fase 3 | POSTECH Data Analytics

## 1. Identificação

| Campo | Valor |
|---|---|
| Turma | Pós Tech - Data Analytics (14DTAT) |
| Grupo | Individual |
| Data de entrega | 03/11/2026 |

### Integrantes

| Nome completo | RM | E-mail |
|---|---|---|
| Luis Guilherme Betarelli Lopes | RM374939 | rm374939@fiap.com.br / luisguilhermeblopes@gmail.com |

---

## 2. Links da entrega

| Item | Link |
|---|---|
| Repositório | https://github.com/lguilherme09/Tech-Challenge-3-FIAP-.git |
| Apresentação executiva (PPT/PDF) | [docs/Tech_Challenge_Fase3_Apresentacao.pptx](docs/Tech_Challenge_Fase3_Apresentacao.pptx) |

---

## 3. O problema

Uma instituição financeira de grande porte quer expandir sua área de Dados, Analytics e IA e precisa entender o mercado brasileiro para definir **contratação, capacitação e investimentos em tecnologia**. A consultoria construiu, na AWS, um pipeline completo de Big Data sobre a pesquisa **State of Data Brazil** (Data Hackers + Bain) e respondeu às perguntas de negócio:

1. Como está estruturado o mercado brasileiro de Dados?
2. Quais perfis profissionais são mais valorizados?
3. Qual é o cenário de diversidade de gênero?
4. Quais tecnologias têm maior adoção?
5. Qual é o índice de adoção de IA e seu impacto?
6. Há diferenças relevantes por região, senioridade ou modelo de trabalho?
7. Quais oportunidades e desafios para quem quer investir em Dados e IA?

---

## 4. Dataset

| Campo | Valor |
|---|---|
| Fonte | [Data Hackers no Kaggle](https://www.kaggle.com/datahackers/datasets) — State of Data Brazil |
| Edições usadas (3 últimas) | 2023 · 2024-2025 · 2025-2026 |
| Linhas × colunas | 2023: 5.293 × 399 · 2024-25: 5.217 × 403 · 2025-26: 3.495 × 388 (**14.005 respostas**) |
| Licença | CC BY-NC-SA 4.0 (Data Hackers e Bain & Company) |

**Principais variáveis harmonizadas na Silver** (de-para completo em `glue_jobs/job_02_silver.py`):

| Coluna harmonizada | 2023 | 2024-25 | 2025-26 | Tratamento |
|---|---|---|---|---|
| genero, regiao, uf | P1_b, P1_i_* | 1.b, 1.i.* | 1.b, 1.i.* | igual nos 3 anos |
| cargo | P2_f | 2.f | 2.f | rótulos curtos; Eng. e Arquiteto de Dados unificados; gestores = "Gestor" |
| nivel / nivel_comparavel | P2_g | 2.g | 2.g | Especialista/Staff+ (só 2025-26) somado a Sênior apenas no comparável |
| faixa_salarial (+ ordem 1..13) | P2_h | 2.h | 2.h | 2 erros de digitação da base corrigidos |
| modelo_trabalho / modelo_ideal | P2_r / P2_s | 2.r / 2.s | **2.q / 2.r** | rótulos curtos |
| ia_prioridade_empresa | P3_e | 3.e | 3.e | respondida só por gestores |
| ia_uso__* (5 flags) | P4_m_* | 4.m.* | **4.j.*** | — |
| lang__*, db__*, cloud__*, bi__* | P4_d/g/h/j | 4.d/g/h/j | **4.c/d/e/g** | flags 0/1; opção inexistente no ano = nulo |

---

## 5. Arquitetura (AWS Academy)

```
Kaggle (3 CSVs) ──► S3 landing/ ──Glue Job 01──► S3 bronze/ ──Glue Job 02──► S3 silver/ ──Glue Job 03──► S3 gold/
                                       │                  │                  │                 │
                                       └──────────► Glue Data Catalog (sod_bronze · sod_silver · sod_gold)
                                                                │                               │
                                                          Amazon Athena (SQL)            Glue Notebook (gráficos)
                                                                └──────────► Material executivo (PPT/PDF)
```

Diagrama completo e editável: `docs/arquitetura_aws.drawio` (abrir em [app.diagrams.net](https://app.diagrams.net)).

| Camada | Conteúdo |
|---|---|
| **Landing** | CSVs originais, como baixados do Kaggle |
| **Bronze** | Parquet/Snappy fiel à origem (tudo string), nomes de coluna sanitizados, colunas de linhagem `_edicao`, `_arquivo_origem`, `_data_ingestao` |
| **Silver** | `sod_silver.respondentes`: 3 edições unidas, 135 colunas padronizadas, particionada por `edicao` |
| **Gold** | 8 tabelas analíticas no formato `edicao × dimensao × métrica` |

Tabelas Gold: `gold_resumo_edicao`, `gold_perfil_mercado`, `gold_salario`, `gold_salario_distribuicao`, `gold_diversidade_genero`, `gold_tecnologias`, `gold_ia_adocao`, `gold_ia_empresa`.

---

## 6. Como reproduzir

### 6.1 Local (PySpark)

```bash
git clone <url-do-repositorio>
cd <repositorio>
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt  # requer Java 11+ para o Spark
```

Coloque os CSVs em `data/raw/` com os nomes abaixo (veja `data/README.md`) e rode em ordem:

```bash
python glue_jobs/job_01_bronze.py
python glue_jobs/job_02_silver.py
python glue_jobs/job_03_gold.py
```

Ou execute os notebooks, na mesma ordem:

| # | Notebook | O que faz |
|---|---|---|
| 1 | `notebooks/01_ingestao_bronze.ipynb` | Landing (CSV) → Bronze (Parquet) |
| 2 | `notebooks/02_silver_harmonizacao.ipynb` | De-para e harmonização das 3 edições |
| 3 | `notebooks/03_gold_tabelas_analiticas.ipynb` | Tabelas analíticas Gold |
| 4 | `notebooks/04_consultas_e_graficos.ipynb` | Consultas SQL e gráficos (`docs/graficos/`) |

### 6.2 AWS Academy (S3 + Glue + Athena)

1. **Iniciar o lab** no AWS Academy e abrir o console (região `us-east-1`).
2. **S3**: criar o bucket `lab-<id-da-conta>` e as pastas abaixo, subindo cada CSV na sua edição:
   - `landing/state_of_data/edicao=2023/state_of_data_2023.csv`
   - `landing/state_of_data/edicao=2024_2025/state_of_data_2024_2025.csv`
   - `landing/state_of_data/edicao=2025_2026/state_of_data_2025_2026.csv`
   - `athena-results/` (vazia, para o Athena)
3. **Glue → ETL jobs → Script editor (Spark)**: criar um job para cada script de `glue_jobs/` (`job_01_bronze`, `job_02_silver`, `job_03_gold`). Em *Job details*:
   - IAM role: **LabRole**
   - Glue version: 5.0 (4.0 também funciona); Worker type G.1X; 2 workers
   - *Advanced properties → Job parameters*: `--BUCKET` = `lab-<id-da-conta>`
   - Marcar **Use Glue Data Catalog as the Hive metastore**
4. Rodar os jobs **na ordem 01 → 02 → 03** e conferir em *Data Catalog → Databases* os bancos `sod_bronze`, `sod_silver` e `sod_gold`.
5. **Athena**: em *Settings*, apontar o resultado para `s3://lab-<id-da-conta>/athena-results/` e executar `sql/athena_consultas_analiticas.sql`. Se a Silver aparecer vazia, rode `MSCK REPAIR TABLE sod_silver.respondentes;`.
6. **Glue Notebook** (alternativa aos jobs e etapa de gráficos): *Glue → Notebooks → Upload*, role **LabRole**, ajustar `BUCKET` na célula de parâmetros e descomentar a célula de magics `%glue_version` / `%worker_type`.

---

## 7. Resultados

| KPI | 2023 | 2024-25 | 2025-26 |
|---|---|---|---|
| Respondentes | 5.293 | 5.215 | 3.494 |
| % mulheres | 24,4% | 23,5% | 22,0% |
| % 100% remoto | 46,3% | 45,7% | 39,7% |
| % que usa IA generativa no trabalho | 80,3% | 93,5% | 97,9% |
| % com IA paga pela empresa | 6,4% | 19,3% | 42,3% |
| Faixa salarial mediana | R$ 8-12k | R$ 8-12k | R$ 8-12k |
| % acima de R$ 16 mil/mês | 16,3% | 21,9% | 27,0% |

**Como ler os números:**

- A amostra é autosselecionada (comunidade Data Hackers), não um censo.
- Salário existe só em faixas e em valores nominais.
- Cada % usa como base quem respondeu aquela pergunta.

---

## 8. Principais conclusões

1. **Estrutura:** analistas, cientistas e engenheiros de dados são 45% do mercado. **Finanças/Bancos é o setor nº 1** nas 3 edições (18,5% em 2025-26).
2. **Maturidade:** a fatia de júniores caiu de 22% para 16%, e a de sêniores (incluindo Staff+) subiu de 30% para 37%. A porta de entrada estreitou.
3. **Valorização:** 50% dos engenheiros de ML/IA e 59% dos gestores ganham acima de R$ 16 mil. Entre os sêniores, a fatia passou de 21% para 36%. Entre quem trabalha para empresa estrangeira, chega a 83%.
4. **Diversidade:** a participação de mulheres caiu de 24% para 22% e diminui a cada nível de carreira. Entre sêniores, 24% das mulheres ganham acima de R$ 16 mil, contra 39% dos homens.
5. **Tecnologias:** Python (92%) e SQL (84%) são a base, e Power BI (59%) domina o BI. **A AWS assumiu a liderança em cloud** (31% → 48%), e o Databricks cresceu de 21% para 33%.
6. **IA:** o uso individual é quase universal (98%), e a empresa já paga a ferramenta para 42% dos profissionais (56% em finanças). Para 61% dos gestores, IA é prioridade. As maiores barreiras são falta de expertise, dados não prontos e ROI não comprovado (de 15% para 28%).
7. **Região e modelo de trabalho:** o presencial subiu de 17% para 21%, mas só 2% o preferem. Acima de R$ 16 mil estão 33% dos remotos contra 12% dos presenciais, e 30% no Sudeste contra 16% no Nordeste.

### Recomendações para a instituição

| Frente | Recomendações |
|---|---|
| **Contratar** | Sêniores de engenharia de dados e ML/IA com pacote no topo do mercado; híbrido flexível ou remoto como alavanca de atração; busca fora do Sudeste |
| **Capacitar** | Academia júnior → pleno; trilhas de Python, SQL, AWS e Databricks; programa de carreira e liderança feminina |
| **Investir** | IA generativa corporativa e governada; dados prontos para IA (qualidade e catálogo); portfólio de casos de uso com metas de ROI |

### Limitações e próximos passos

- Corrigir os salários pelo IPCA.
- Incluir as edições 2021-22 para ter séries mais longas.
- Publicar um dashboard (QuickSight) sobre a camada Gold.
- Orquestrar os jobs com Glue Workflows.

---

## 9. Estrutura do repositório

```
.
├── data/                 CSVs brutos (raw) — não versionados, ver data/README.md
├── glue_jobs/            scripts PySpark dos Glue Jobs (Bronze, Silver, Gold)
├── notebooks/            mesmos passos em notebook (Glue Notebook / local) + consultas e gráficos
├── sql/                  consultas analíticas do Athena
└── docs/                 apresentação executiva (PPTX/PDF), diagrama Draw.io e gráficos
```

---

## 10. Tecnologias

- **AWS:** Amazon S3 (Data Lake), AWS Glue (Jobs Spark, Notebooks, Data Catalog), Amazon Athena, IAM (LabRole)
- **Apache Spark / PySpark 3.5:** processamento distribuído, Spark SQL e funções de janela
- **Parquet + Snappy:** formato colunar das camadas Bronze, Silver e Gold
- **Python:** pandas e matplotlib (gráficos)
- **Draw.io:** diagrama da arquitetura
- **PowerPoint:** material executivo com DataViz e storytelling
