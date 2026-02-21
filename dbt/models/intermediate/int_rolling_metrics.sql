{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'batch']
    )
}}

/*
    Computes rolling technical indicators per symbol:
      - Simple Moving Averages (5, 10, 20, 50-day)
      - Rolling volatility (20-day std dev of returns)
      - Rolling avg volume
      - RSI-14 approximation

    Ephemeral — compiled inline into downstream marts.
*/

with returns as (
    select * from {{ ref('int_daily_returns') }}
),

rolling as (
    select
        symbol,
        trade_date,
        close_price,
        volume,
        daily_return_pct,
        intraday_range_pct,
        symbol_row_num,

        -- ── Simple Moving Averages ──────────────────────────────────────────
        round(avg(close_price) over (
            partition by symbol
            order by trade_date
            rows between 4 preceding and current row
        ), 4)                                   as sma_5d,

        round(avg(close_price) over (
            partition by symbol
            order by trade_date
            rows between 9 preceding and current row
        ), 4)                                   as sma_10d,

        round(avg(close_price) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ), 4)                                   as sma_20d,

        round(avg(close_price) over (
            partition by symbol
            order by trade_date
            rows between 49 preceding and current row
        ), 4)                                   as sma_50d,

        -- ── Volatility (annualised from 20-day return std dev) ──────────────
        round(stddev(daily_return_pct) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ), 4)                                   as volatility_20d_pct,

        round(stddev(daily_return_pct) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ) * sqrt(252), 4)                       as annualised_volatility_pct,

        -- ── Volume ─────────────────────────────────────────────────────────
        round(avg(volume) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ), 0)                                   as avg_volume_20d,

        round(volume / nullif(avg(volume) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ), 0), 4)                               as volume_vs_avg_20d,

        -- ── RSI-14 Approximation ────────────────────────────────────────────
        -- Uses average gain/loss over 14 periods (Wilder smoothing simplified)
        round(
            100 - (
                100 / (
                    1 + nullif(
                        avg(case when daily_return_pct > 0 then daily_return_pct else 0 end) over (
                            partition by symbol
                            order by trade_date
                            rows between 13 preceding and current row
                        ) /
                        nullif(
                            avg(case when daily_return_pct < 0 then abs(daily_return_pct) else 0 end) over (
                                partition by symbol
                                order by trade_date
                                rows between 13 preceding and current row
                            ),
                        0),
                    0)
                )
            ),
        2)                                      as rsi_14

    from returns
)

select * from rolling
