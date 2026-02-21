{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'streaming']
    )
}}

/*
    Enriches the real-time streaming staging data with:
      - Price momentum signals derived from MA crossovers
      - Volatility regime classification (low / normal / elevated / high)
      - Volume spike detection
      - Previous window comparison for delta calculations

    Ephemeral — compiled inline into mart_realtime_signals.
*/

with base as (
    select * from {{ ref('stg_realtime_stock_analytics') }}
),

with_deltas as (
    select
        symbol,
        window_start_at,
        window_end_at,
        window_duration_minutes,
        ma_15m,
        ma_1h,
        volatility_15m,
        volume_15m,
        stream_loaded_at,

        -- ── MA deltas vs previous window ───────────────────────────────────
        round(
            ma_15m - lag(ma_15m) over (
                partition by symbol order by window_start_at
            ),
            4
        )                               as ma_15m_delta,

        round(
            ma_1h - lag(ma_1h) over (
                partition by symbol order by window_start_at
            ),
            4
        )                               as ma_1h_delta,

        -- ── MA spread (15m vs 1h): positive = short-term upward momentum ───
        round(ma_15m - ma_1h, 4)        as ma_spread,

        -- ── Volume avg over last 4 windows (~1 hour) ───────────────────────
        round(avg(volume_15m) over (
            partition by symbol
            order by window_start_at
            rows between 3 preceding and current row
        ), 0)                           as avg_volume_1h,

        -- ── Volatility avg over last 4 windows ────────────────────────────
        round(avg(volatility_15m) over (
            partition by symbol
            order by window_start_at
            rows between 3 preceding and current row
        ), 4)                           as avg_volatility_1h

    from base
),

with_signals as (
    select
        *,

        -- ── Momentum signal ────────────────────────────────────────────────
        case
            when ma_spread > 0 and ma_15m_delta > 0  then 'BULLISH'
            when ma_spread < 0 and ma_15m_delta < 0  then 'BEARISH'
            else 'NEUTRAL'
        end                             as momentum_signal,

        -- ── Volatility regime ──────────────────────────────────────────────
        case
            when volatility_15m >= avg_volatility_1h * 2.0  then 'HIGH'
            when volatility_15m >= avg_volatility_1h * 1.5  then 'ELEVATED'
            when volatility_15m <= avg_volatility_1h * 0.5  then 'LOW'
            else 'NORMAL'
        end                             as volatility_regime,

        -- ── Volume spike flag ──────────────────────────────────────────────
        case
            when avg_volume_1h > 0
             and volume_15m >= avg_volume_1h * 2.0   then true
            else false
        end                             as is_volume_spike

    from with_deltas
)

select * from with_signals
