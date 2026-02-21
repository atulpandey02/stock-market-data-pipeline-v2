# dbt Transformation Layer — Stock Market Data Pipeline

This folder contains the **dbt transformation layer** that sits between your
Snowflake raw tables (loaded by Spark/Airflow) and your analytics-ready marts.

---

## Architecture

```
Snowflake Raw Tables              dbt Layers                    Outputs
─────────────────────   ──────────────────────────────   ─────────────────────
STOCKMARKETBATCH        staging/  → intermediate/ →       mart_stock_performance
  DAILY_STOCK_METRICS   (views)     (ephemeral)            mart_daily_summary
                                                           
STOCKMARKETSTREAM       staging/  → intermediate/ →       mart_realtime_signals
  REALTIME_STOCK_ANALYTICS(views)   (ephemeral)           
```

---

## Models

### Staging (`models/staging/`)
| Model | Source | Description |
|---|---|---|
| `stg_daily_stock_metrics` | STOCKMARKETBATCH | Cleaned daily OHLCV data |
| `stg_realtime_stock_analytics` | STOCKMARKETSTREAM | Cleaned 15-min window metrics |

### Intermediate (`models/intermediate/`)
| Model | Description |
|---|---|
| `int_daily_returns` | Daily price returns, overnight gaps, intraday range |
| `int_rolling_metrics` | SMAs, volatility, volume ratio, RSI-14 |
| `int_realtime_enriched` | Momentum signals, volatility regime, volume spikes |

### Marts (`models/marts/`)
| Model | Grain | Description |
|---|---|---|
| `mart_stock_performance` | Symbol × Day | Full technical indicator history |
| `mart_daily_summary` | Day | Market-wide breadth and top movers |
| `mart_realtime_signals` | Symbol × 15-min window | Live signals + GenAI-ready summaries |

---

## Setup

### 1. Install dbt
```bash
pip install dbt-snowflake==1.7.*
```

### 2. Install dbt packages
```bash
cd dbt/
dbt deps
```

### 3. Configure environment variables
Ensure these are set (already in your `.env`):
```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_ROLE       (optional, defaults to SYSADMIN)
```

### 4. Test connection
```bash
dbt debug --profiles-dir .
```

---

## Running dbt

```bash
# Run everything
dbt run --profiles-dir .

# Run only staging
dbt run --select tag:staging --profiles-dir .

# Run only marts
dbt run --select tag:mart --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Check source freshness
dbt source freshness --profiles-dir .

# Generate & serve docs locally
dbt docs generate --profiles-dir .
dbt docs serve
```

---

## Airflow Integration

Copy `dbt_transformation_dag.py` to `src/airflow/dags/` and trigger it after
the batch/stream Snowflake load tasks complete.

The batch DAG task chain becomes:
```
load_historical_to_snowflake → [triggers] → dbt_transformation_pipeline DAG
```

---

## GenAI Integration (Future)

The marts are designed as GenAI-ready:

- **`mart_realtime_signals.signal_summary`** — plain-text field summarising
  each window's signals, designed for injection into LLM prompts
- **`mart_daily_summary`** — daily market digest fields perfect for
  auto-generating analyst commentary
- **`mart_stock_performance`** — structured technical data for RAG retrieval
- **dbt docs** — schema descriptions feed directly into LLM system prompts
  as structured context about what each column means

---

## Project Structure

```
dbt/
├── dbt_project.yml          # Project config
├── profiles.yml             # Snowflake connection (reads from env vars)
├── packages.yml             # dbt-utils, dbt-expectations
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   ├── stg_daily_stock_metrics.sql
│   │   └── stg_realtime_stock_analytics.sql
│   ├── intermediate/
│   │   ├── int_daily_returns.sql
│   │   ├── int_rolling_metrics.sql
│   │   └── int_realtime_enriched.sql
│   └── marts/
│       ├── schema.yml
│       ├── mart_stock_performance.sql
│       ├── mart_realtime_signals.sql
│       └── mart_daily_summary.sql
├── tests/
│   ├── assert_high_not_below_low.sql
│   ├── assert_no_future_trades.sql
│   └── assert_stream_window_integrity.sql
├── macros/
│   ├── safe_divide.sql
│   └── generate_schema_name.sql
├── seeds/
│   └── sp500_tickers.csv
└── dbt_transformation_dag.py   # → copy to src/airflow/dags/
```
