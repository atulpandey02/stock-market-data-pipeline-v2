{{
    config(
        materialized='table',
        tags=['mart', 'streaming'],
        cluster_by=['symbol', 'window_start_at']
    )
}}

/*
    Real-time analytics mart surfacing enriched streaming signals.
    Designed for live dashboards and as the primary feed for the
    future GenAI analyst layer.

    Grain: one row per symbol per 15-minute window.
    Consumers: real-time dashboards, alerting, GenAI context layer.
*/

with enriched as (
    select * from {{ ref('int_realtime_enriched') }}
)

select
    -- ── Identifiers ────────────────────────────────────────────────────────
    symbol,
    window_start_at,
    window_end_at,
    window_duration_minutes,

    -- ── Price signals ──────────────────────────────────────────────────────
    ma_15m,
    ma_1h,
    ma_spread,
    ma_15m_delta,
    ma_1h_delta,
    momentum_signal,

    -- ── Volatility ─────────────────────────────────────────────────────────
    volatility_15m,
    avg_volatility_1h,
    volatility_regime,

    -- ── Volume ─────────────────────────────────────────────────────────────
    volume_15m,
    avg_volume_1h,
    is_volume_spike,

    -- ── Composite alert flag ───────────────────────────────────────────────
    -- True when multiple signals align — useful for GenAI to surface highlights
    case
        when momentum_signal != 'NEUTRAL'
         and volatility_regime in ('ELEVATED', 'HIGH')
         and is_volume_spike = true then true
        else false
    end                             as is_multi_signal_alert,

    -- ── Human-readable summary (GenAI-ready context field) ────────────────
    symbol
        || ' | ' || momentum_signal
        || ' | Vol: ' || volatility_regime
        || case when is_volume_spike then ' | VOLUME SPIKE' else '' end
        || ' @ ' || to_varchar(window_start_at, 'YYYY-MM-DD HH24:MI')
                                    as signal_summary,

    -- ── Metadata ───────────────────────────────────────────────────────────
    stream_loaded_at,
    current_timestamp()             as dbt_updated_at

from enriched
