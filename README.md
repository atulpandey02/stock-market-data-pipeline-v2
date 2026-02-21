# Stock Market Data Pipeline v2

[![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://snowflake.com)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)

An end-to-end Data Engineering project processing real-time and historical stock market data.
**v2 adds a full dbt transformation layer** on top of the original Kafka → Spark → Snowflake pipeline,
enabling versioned, tested, and documented analytics transformations in Snowflake.

---

## What's New in v2

- **dbt transformation layer** — staging, intermediate (ephemeral), and mart models
- **Technical indicators** — SMA 5/10/20/50d, RSI-14, annualised volatility, MA crossover signals
- **Real-time signals mart** — momentum, volatility regime, volume spike detection
- **Market-wide daily summary** — breadth, top movers, golden/death cross counts
- **Automated dbt tests** — schema tests, custom singular tests, freshness checks
- **Full dbt documentation** — all columns described, generated via `dbt docs`
- **dbt Airflow DAG** — wired into the batch pipeline, runs after every Snowflake load
- **GenAI-ready** — `signal_summary` field and structured marts designed for LLM context injection

---

## Architecture

```
Yahoo Finance / Alpha Vantage
        │
        ▼
  Kafka Producers ──────────────────────────────────┐
  (stream + batch)                                  │
        │                                           │
        ▼                                           ▼
  Kafka Topics                               Kafka Topics
  (realtime)                                 (batch)
        │                                           │
        ▼                                           ▼
  Spark Structured              Spark Batch Processor
  Streaming                     (spark_batch_processor.py)
        │                                           │
        ▼                                           ▼
  MinIO processed/realtime/     MinIO processed/historical/
        │                                           │
        ▼                                           ▼
  STOCKMARKETSTREAM             STOCKMARKETBATCH
  .REALTIME_STOCK_ANALYTICS     .DAILY_STOCK_METRICS
        │                                           │
        └──────────────┬────────────────────────────┘
                       ▼
              ┌─────────────────┐
              │   dbt Layer     │
              │                 │
              │  stg_ models    │  ← views, clean + cast
              │  int_ models    │  ← ephemeral, business logic
              │  mart_ models   │  ← tables, analytics-ready
              └────────┬────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
  mart_stock_   mart_daily_  mart_realtime_
  performance   summary      signals
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka 2.8, Yahoo Finance API |
| Processing | Apache Spark 3.3 (batch + streaming) |
| Storage | MinIO (S3-compatible data lake) |
| Warehouse | Snowflake |
| Transformation | **dbt-snowflake 1.7** |
| Orchestration | Apache Airflow 2.5 |
| Infrastructure | Docker + Docker Compose |

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/atulpandey02/stock-market-data-pipeline-v2.git
cd stock-market-data-pipeline-v2

# 2. Configure
cp .env.example .env
# Edit .env with your Snowflake credentials

# 3. Start all services
docker-compose up -d

# 4. Access
# Airflow UI:  http://localhost:8080  (admin/admin)
# MinIO:       http://localhost:9001  (minioadmin/minioadmin)
# Kafka UI:    http://localhost:8081
```

---

## dbt Setup

```bash
# Install dbt
pip install dbt-snowflake==1.7.0

# Install dbt packages
cd dbt && dbt deps

# Test connection
dbt debug --profiles-dir .

# Run all models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Generate & serve docs
dbt docs generate --profiles-dir . && dbt docs serve
```

---

## dbt Models

### Staging (views)
| Model | Source DB | Description |
|---|---|---|
| `stg_daily_stock_metrics` | STOCKMARKETBATCH | Cleaned OHLCV daily data |
| `stg_realtime_stock_analytics` | STOCKMARKETSTREAM | Cleaned 15-min window metrics |

### Intermediate (ephemeral)
| Model | Description |
|---|---|
| `int_daily_returns` | Daily returns, overnight gaps, intraday range |
| `int_rolling_metrics` | SMA 5/10/20/50d, RSI-14, annualised volatility |
| `int_realtime_enriched` | Momentum signals, volatility regime, volume spikes |

### Marts (tables)
| Model | Grain | Key Columns |
|---|---|---|
| `mart_stock_performance` | Symbol × Day | SMAs, RSI, MA signals, 52-week range |
| `mart_daily_summary` | Day | Breadth, top movers, cross counts |
| `mart_realtime_signals` | Symbol × 15-min | momentum_signal, volatility_regime, signal_summary |

---

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `stock_market_batch_pipeline` | Daily 02:00 UTC | Fetch → Kafka → Spark → Snowflake → triggers dbt |
| `stock_streaming_pipeline` | Every 30 min | Realtime collect → Spark → Snowflake |
| `dbt_transformation_pipeline` | Daily 06:00 UTC | dbt deps → source freshness → run → test → docs |

---

## Folder Structure

```
stock-market-data-pipeline-v2/
├── docker-compose.yaml
├── requirements.txt
├── .env
├── src/
│   ├── airflow/dags/
│   │   ├── stock_market_batch_dag.py
│   │   ├── stock_market_stream_dag.py
│   │   ├── dbt_transformation_dag.py      ← NEW
│   │   └── scripts/
│   │       ├── batch_data_producer.py
│   │       ├── batch_data_consumer.py
│   │       ├── stream_data_producer.py
│   │       ├── realtime_data_consumer.py
│   │       ├── load_to_snowflake.py
│   │       └── load_stream_to_snowflake.py
│   ├── spark/jobs/
│   │   ├── spark_batch_processor.py
│   │   └── spark_stream_batch_processor.py
│   └── snowflake/scripts/
├── dbt/                                   ← NEW
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   ├── macros/
│   └── seeds/
└── data/
```

---

## GenAI Layer (Coming Soon)

The marts are purpose-built for a future GenAI analyst layer:
- `mart_realtime_signals.signal_summary` → plain-text LLM prompt context
- `mart_daily_summary` → daily market digest auto-generation
- `mart_stock_performance` → RAG retrieval for technical analysis queries
- dbt column descriptions → LLM system prompt schema context

---

*Built on [stock-market-data-pipeline](https://github.com/atulpandey02/stock-market-data-pipeline) with dbt transformation layer added in v2.*
