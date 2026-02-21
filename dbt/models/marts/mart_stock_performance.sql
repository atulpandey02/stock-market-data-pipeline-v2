{{
    config(
        materialized='table',
        tags=['mart', 'batch'],
        cluster_by=['symbol', 'trade_date']
    )
}}

/*
    Primary analytics mart for historical daily stock performance.
    Combines price returns + rolling technical indicators into one
    wide table optimised for BI tools and future GenAI querying.

    Grain: one row per symbol per trading day.
    Consumers: dashboards, reporting, GenAI context layer.
*/

with rolling as (
    select * from {{ ref('int_rolling_metrics') }}
),

returns as (
    select * from {{ ref('int_daily_returns') }}
),

joined as (
    select
        -- ── Identifiers ────────────────────────────────────────────────────
        r.symbol,
        r.trade_date,

        -- ── OHLCV ──────────────────────────────────────────────────────────
        r.open_price,
        r.high_price,
        r.low_price,
        r.close_price,
        r.volume,

        -- ── Returns ────────────────────────────────────────────────────────
        r.prev_close_price,
        r.daily_return_pct,
        r.intraday_range,
        r.intraday_range_pct,
        r.overnight_gap,

        -- ── Moving Averages ────────────────────────────────────────────────
        m.sma_5d,
        m.sma_10d,
        m.sma_20d,
        m.sma_50d,

        -- ── MA crossover signals ───────────────────────────────────────────
        case
            when m.sma_5d > m.sma_20d  then 'GOLDEN_CROSS'
            when m.sma_5d < m.sma_20d  then 'DEATH_CROSS'
            else 'NEUTRAL'
        end                             as ma_signal_5_20,

        case
            when m.sma_10d > m.sma_50d then 'GOLDEN_CROSS'
            when m.sma_10d < m.sma_50d then 'DEATH_CROSS'
            else 'NEUTRAL'
        end                             as ma_signal_10_50,

        -- ── Volatility ─────────────────────────────────────────────────────
        m.volatility_20d_pct,
        m.annualised_volatility_pct,

        -- ── Volume ─────────────────────────────────────────────────────────
        m.avg_volume_20d,
        m.volume_vs_avg_20d,
        case
            when m.volume_vs_avg_20d >= 2.0 then true
            else false
        end                             as is_high_volume_day,

        -- ── RSI ────────────────────────────────────────────────────────────
        m.rsi_14,
        case
            when m.rsi_14 >= 70 then 'OVERBOUGHT'
            when m.rsi_14 <= 30 then 'OVERSOLD'
            else 'NEUTRAL'
        end                             as rsi_signal,

        -- ── 52-week high/low ───────────────────────────────────────────────
        max(r.high_price) over (
            partition by r.symbol
            order by r.trade_date
            rows between 251 preceding and current row
        )                               as high_52w,

        min(r.low_price) over (
            partition by r.symbol
            order by r.trade_date
            rows between 251 preceding and current row
        )                               as low_52w,

        round(
            (r.close_price - min(r.low_price) over (
                partition by r.symbol
                order by r.trade_date
                rows between 251 preceding and current row
            )) / nullif(
                max(r.high_price) over (
                    partition by r.symbol
                    order by r.trade_date
                    rows between 251 preceding and current row
                ) - min(r.low_price) over (
                    partition by r.symbol
                    order by r.trade_date
                    rows between 251 preceding and current row
                ),
            0) * 100,
        2)                              as pct_of_52w_range,

        -- ── Metadata ───────────────────────────────────────────────────────
        r.batch_loaded_at,
        current_timestamp()             as dbt_updated_at

    from returns r
    inner join rolling m
        on r.symbol = r.symbol
        and r.trade_date = m.trade_date
        and r.symbol = m.symbol
)

select * from joined
