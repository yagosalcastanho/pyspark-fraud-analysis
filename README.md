# PySpark Fraud Analysis

**[Português](#português) • [English](#english)**

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=flat-square&logo=apachespark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![Jupyter](https://img.shields.io/badge/Jupyter-Lab-F37626?style=flat-square&logo=jupyter)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## Português

Análise de 1,29 milhão de transações de cartão de crédito usando PySpark em ambiente Docker. O projeto processa um dataset real de detecção de fraude, aplica transformações distribuídas, window functions e agregações analíticas, persiste os resultados em PostgreSQL e Parquet, e entrega um sumário executivo com descobertas e recomendações de negócio.

### O Problema

Fraude em cartões de crédito é rara em volume — menos de 1% das transações — mas concentrada em valores altos e em padrões específicos de comportamento. Identificar esses padrões em escala requer processamento distribuído. Este projeto responde quatro perguntas de negócio concretas usando 1,29 milhão de transações reais.

### Dataset

[Credit Card Transactions Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection) — Kaggle

- 1.296.675 transações entre janeiro de 2019 e junho de 2020
- 7.506 transações fraudulentas (0,58% do total)
- Colunas: timestamp, cartão, merchant, categoria, valor, localização do portador, localização do merchant, flag de fraude

O dataset é altamente desbalanceado — o que é realista. No mundo real, fraude é rara mas o impacto financeiro é alto: $3,99 milhões em volume fraudulento neste dataset.

### Arquitetura

```
fraudTrain.csv (500MB)
       |
       v
  PySpark + Jupyter (container: spark_jupyter)
       |
       |-- Ingest       schema explícito, sem inferência dupla
       |-- Transform    colunas derivadas, distância geográfica, segmentos
       |-- Enrich       broadcast join com tabela de categorias
       |-- Window       lag, row_number, médias móveis de 7 dias
       |-- Aggregate    fraude por categoria, hora, demografia, estado
       |
       |-----> Parquet particionado por ano/mês (output/parquet/)
       |-----> PostgreSQL 15 (container: postgres_spark)
       |-----> Relatório PNG (output/fraud_analysis_report.png)
```

### Tecnologias

| Camada | Tecnologia | Uso |
|---|---|---|
| Processamento | PySpark 3.5.0 | Engine distribuída para 1.29M de registros |
| Notebook | JupyterLab | Desenvolvimento interativo com Spark UI |
| Banco de dados | PostgreSQL 15 | Persistência das agregações via JDBC |
| Formato de saída | Parquet | Armazenamento colunar particionado |
| Infraestrutura | Docker Compose | Ambiente reproduzível |

### Estrutura do Projeto

```
pyspark-fraud-analysis/
├── notebooks/
│   └── Fraud_analysis.ipynb    # notebook principal com 12 células
├── data/                       # dataset CSV (não versionado)
├── output/
│   ├── parquet/                # dados particionados por ano/mês
│   └── fraud_analysis_report.png
├── sql/
│   └── create_tables.sql
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

### Quickstart

**Pré-requisitos:** Docker e Docker Compose instalados. Dataset `fraudTrain.csv` do Kaggle em `data/`.

```bash
git clone https://github.com/yagosalcastanho/pyspark-fraud-analysis.git
cd pyspark-fraud-analysis

# baixe o dataset e coloque em data/fraudTrain.csv

docker compose up -d
```

Acesse `http://localhost:8888` com o token exibido em `docker compose logs spark_jupyter`.

Abra `notebooks/Fraud_analysis.ipynb` e execute as células em ordem com `Shift+Enter`.

O Spark UI fica disponível em `http://localhost:4040` durante a execução — mostra os jobs, stages e tasks em tempo real.

### Técnicas utilizadas

**Schema explícito na ingestão** — evita que o Spark leia o arquivo duas vezes para inferir tipos. Em arquivos de 500MB, isso representa uma passada a menos no disco.

**Broadcast join** — a tabela de categorias (14 linhas) é enviada para todos os workers em memória via `F.broadcast()`, eliminando o shuffle que um join convencional causaria entre 1.29M de registros.

**Window functions** — `row_number()` para sequência por cartão, `lag()` para valor anterior, `rangeBetween(-7*86400, 0)` para médias móveis de 7 dias. Essas funções calculam métricas por grupo mantendo o nível de linha, o que é impossível com `groupBy` simples.

**Particionamento físico em Parquet** — dados escritos com `partitionBy("trans_year", "trans_month")`. Uma consulta `WHERE trans_year=2020 AND trans_month=1` lê apenas a pasta correspondente, ignorando todos os outros dados.

**Cache estratégico** — `df_raw.cache()` e `df_clean.cache()` mantêm os DataFrames em memória após a primeira action. O impacto medido foi de ~3s para ~0.5s na mesma operação de contagem.

**Adaptive Query Execution** — `spark.sql.adaptive.enabled=true` permite que o Spark replaneje joins e coalesce partições em tempo de execução com base nos dados reais.

### Storytelling dos dados

**Pergunta 1: Quais categorias concentram a fraude?**

E-commerce (`shopping_net`) lidera com 1,76% de taxa de fraude e ticket médio fraudulento de $999 — 14 vezes maior que o ticket de combustível. Internet geral (`misc_net`) aparece em segundo com 1,45%. O padrão é claro: fraudadores preferem categorias online de alto valor onde não há verificação física.

**Pergunta 2: Quando a fraude acontece?**

Entre meia-noite e 3h a taxa de fraude é ~1,5%. Durante o dia cai para ~0,1%. Às 22h e 23h dispara para 2,88% e 2,84% respectivamente — o maior pico do dia. A hipótese é que esses horários combinam menor monitoramento humano com maior tolerância dos sistemas automáticos de aprovação.

**Pergunta 3: Qual é o perfil demográfico da vítima?**

Idosos acima de 60 anos têm a maior taxa de fraude: 0,73% para mulheres e 0,76% para homens. A taxa cai consistentemente nas faixas mais jovens. Isso é consistente com dados externos sobre vulnerabilidade de idosos a golpes financeiros.

**Pergunta 4: Quais estados concentram o volume financeiro?**

Nova York lidera com $295.548 em fraudes, seguida por Texas ($265.806) e Pensilvânia ($244.624). Os três estados juntos representam 20% do volume total de fraude do dataset, reflexo da concentração populacional e de renda.

**Recomendações derivadas da análise:**
- Reforçar autenticação em transações de e-commerce, especialmente acima de $500
- Monitoramento intensivo entre 22h e 3h com revisão manual de transações suspeitas
- Alertas automáticos para variações acima de 300% no valor médio por cartão nos últimos 7 dias
- Revisão de transações com distância portador-merchant acima de 500 km

### Resultados

```
Total de transações analisadas : 1,296,675
Transações fraudulentas        :     7,506
Taxa de fraude global          :     0.579%
Volume financeiro de fraude    : $3,988,088.61
Categoria de maior risco       : E-commerce
Hora de pico da fraude         : 22h
```

---

## English

Analysis of 1.29 million credit card transactions using PySpark in a Docker environment. The project processes a real fraud detection dataset, applies distributed transformations, window functions and analytical aggregations, persists results to PostgreSQL and Parquet, and delivers an executive summary with findings and business recommendations.

### The Problem

Credit card fraud is rare by volume — less than 1% of transactions — but concentrated in high values and specific behavioral patterns. Identifying these patterns at scale requires distributed processing. This project answers four concrete business questions using 1.29 million real transactions.

### Dataset

[Credit Card Transactions Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection) — Kaggle

- 1,296,675 transactions between January 2019 and June 2020
- 7,506 fraudulent transactions (0.58% of total)
- Columns: timestamp, card number, merchant, category, amount, cardholder location, merchant location, fraud flag

The dataset is highly imbalanced — which is realistic. In the real world, fraud is rare but the financial impact is high: $3.99 million in fraudulent volume in this dataset.

### Architecture

```
fraudTrain.csv (500MB)
       |
       v
  PySpark + Jupyter (container: spark_jupyter)
       |
       |-- Ingest       explicit schema, no double inference
       |-- Transform    derived columns, geographic distance, segments
       |-- Enrich       broadcast join with category reference table
       |-- Window       lag, row_number, 7-day moving averages
       |-- Aggregate    fraud by category, hour, demographics, state
       |
       |-----> Parquet partitioned by year/month (output/parquet/)
       |-----> PostgreSQL 15 (container: postgres_spark)
       |-----> PNG Report (output/fraud_analysis_report.png)
```

### Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Processing | PySpark 3.5.0 | Distributed engine for 1.29M records |
| Notebook | JupyterLab | Interactive development with Spark UI |
| Database | PostgreSQL 15 | Aggregation persistence via JDBC |
| Output format | Parquet | Partitioned columnar storage |
| Infrastructure | Docker Compose | Reproducible environment |

### Project Structure

```
pyspark-fraud-analysis/
├── notebooks/
│   └── Fraud_analysis.ipynb    # main notebook with 12 cells
├── data/                       # CSV dataset (not versioned)
├── output/
│   ├── parquet/                # data partitioned by year/month
│   └── fraud_analysis_report.png
├── sql/
│   └── create_tables.sql
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

### Quickstart

**Prerequisites:** Docker and Docker Compose installed. Dataset `fraudTrain.csv` from Kaggle in `data/`.

```bash
git clone https://github.com/yagosalcastanho/pyspark-fraud-analysis.git
cd pyspark-fraud-analysis

# download the dataset and place it at data/fraudTrain.csv

docker compose up -d
```

Access `http://localhost:8888` with the token shown in `docker compose logs spark_jupyter`.

Open `notebooks/Fraud_analysis.ipynb` and run cells in order with `Shift+Enter`.

The Spark UI is available at `http://localhost:4040` during execution — shows jobs, stages and tasks in real time.

### Techniques used

**Explicit schema on ingest** — prevents Spark from reading the file twice to infer types. On 500MB files, this saves one full disk scan.

**Broadcast join** — the category table (14 rows) is sent to all workers in memory via `F.broadcast()`, eliminating the shuffle that a conventional join would cause across 1.29M records.

**Window functions** — `row_number()` for per-card sequence, `lag()` for previous value, `rangeBetween(-7*86400, 0)` for 7-day moving averages. These functions compute per-group metrics while maintaining row-level granularity, which is impossible with `groupBy` alone.

**Physical partitioning in Parquet** — data written with `partitionBy("trans_year", "trans_month")`. A query `WHERE trans_year=2020 AND trans_month=1` reads only the corresponding folder, skipping all other data.

**Strategic caching** — `df_raw.cache()` and `df_clean.cache()` keep DataFrames in memory after the first action. The measured impact was ~3s down to ~0.5s on the same count operation.

**Adaptive Query Execution** — `spark.sql.adaptive.enabled=true` allows Spark to replan joins and coalesce partitions at runtime based on actual data statistics.

### Data Storytelling

**Question 1: Which categories concentrate fraud?**

E-commerce (`shopping_net`) leads with a 1.76% fraud rate and an average fraudulent ticket of $999 — 14 times higher than the gas station ticket. General internet (`misc_net`) ranks second at 1.45%. The pattern is clear: fraudsters prefer high-value online categories where there is no physical verification.

**Question 2: When does fraud happen?**

Between midnight and 3am the fraud rate is ~1.5%. During the day it drops to ~0.1%. At 10pm and 11pm it spikes to 2.88% and 2.84% respectively — the highest peaks of the day. The hypothesis is that these hours combine lower human monitoring with higher tolerance from automated approval systems.

**Question 3: What is the victim demographic profile?**

Adults over 60 have the highest fraud rate: 0.73% for women and 0.76% for men. The rate drops consistently in younger age groups. This is consistent with external data on elderly vulnerability to financial scams.

**Question 4: Which states concentrate financial volume?**

New York leads with $295,548 in fraud, followed by Texas ($265,806) and Pennsylvania ($244,624). The three states together represent 20% of the dataset's total fraud volume, reflecting population and income concentration.

**Recommendations derived from the analysis:**
- Strengthen authentication for e-commerce transactions, especially above $500
- Intensive monitoring between 10pm and 3am with manual review of suspicious transactions
- Automatic alerts for variations above 300% in the average card value over the last 7 days
- Review transactions with cardholder-merchant distance above 500 km

### Results

```
Total transactions analyzed : 1,296,675
Fraudulent transactions     :     7,506
Global fraud rate           :     0.579%
Financial fraud volume      : $3,988,088.61
Highest risk category       : E-commerce
Fraud peak hour             : 10pm
```

---

### Contributing | Contribuindo

Contributions are more than welcome. Fork the project, create a branch, commit your changes and open a pull request, maybe you know something that i don´t know :). 
Contribuições são mais que bem-vindas. Faça um fork, crie uma branch, commite suas alterações e abra um pull request, talvez você saiba de algo que eu não sei.

### License | Licença

Distributed under the MIT License.
Distribuído sob a licença MIT.

---

<div align="center">
  Developed as a Data Engineering portfolio project<br>
  Desenvolvido como projeto de portfólio de Engenharia de Dados
</div>
