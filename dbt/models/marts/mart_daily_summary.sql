{{
    config(
        materialized='table',
        tags=['mart', 'batch'],
        cluster_by=['trade_date']
    )
}}

/*
    Cross-symbol daily market summary.
    Aggregates the entire tracked universe to give a market-wide
    view per day — top movers, breadth, sector volatility snapshot.

    Grain: one row per trading date (market-wide).
    Consumers: executive dashboards, GenAI market summary prompts.
*/

with perf as (
    select * from {{ ref('mart_stock_performance') }}
),

daily_agg as (
    select
        trade_date,

        -- ── Universe size ─────────────────────────────────────────────────
        count(distinct symbol)                                      as symbols_tracked,

        -- ── Market breadth ────────────────────────────────────────────────
        countif(daily_return_pct > 0)                               as advancers,
        countif(daily_return_pct < 0)                               as decliners,
        countif(daily_return_pct = 0)                               as unchanged,
        round(
            countif(daily_return_pct > 0) /
            nullif(count(distinct symbol), 0) * 100,
        1)                                                          as advance_decline_ratio_pct,

        -- ── Return stats ──────────────────────────────────────────────────
        round(avg(daily_return_pct), 4)                             as avg_return_pct,
        round(median(daily_return_pct), 4)                          as median_return_pct,
        round(stddev(daily_return_pct), 4)                          as return_stddev,

        -- ── Top & bottom movers ───────────────────────────────────────────
        max(daily_return_pct)                                       as best_return_pct,
        min(daily_return_pct)                                       as worst_return_pct,

        -- Symbol with highest return
        max_by(symbol, daily_return_pct)                            as top_gainer_symbol,
        -- Symbol with lowest return
        min_by(symbol, daily_return_pct)                            as top_loser_symbol,

        -- ── Volume ────────────────────────────────────────────────────────
        sum(volume)                                                 as total_volume,
        round(avg(volume_vs_avg_20d), 4)                            as avg_volume_vs_norm,
        countif(is_high_volume_day)                                 as high_volume_stocks,

        -- ── Volatility ────────────────────────────────────────────────────
        round(avg(annualised_volatility_pct), 2)                    as avg_annualised_vol_pct,
        round(avg(intraday_range_pct), 4)                           as avg_intraday_range_pct,

        -- ── RSI signals ───────────────────────────────────────────────────
        countif(rsi_signal = 'OVERBOUGHT')                          as overbought_count,
        countif(rsi_signal = 'OVERSOLD')                            as oversold_count,

        -- ── MA signals ────────────────────────────────────────────────────
        countif(ma_signal_5_20 = 'GOLDEN_CROSS')                    as golden_cross_5_20_count,
        countif(ma_signal_5_20 = 'DEATH_CROSS')                     as death_cross_5_20_count,

        -- ── Metadata ──────────────────────────────────────────────────────
        current_timestamp()                                         as dbt_updated_at

    from perf
    group by trade_date
)

select * from daily_agg
order by trade_date desc
