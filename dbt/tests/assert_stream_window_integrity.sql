-- Test: Streaming windows must have end > start, and duration must be positive
-- Fails if any rows are returned

select
    symbol,
    window_start_at,
    window_end_at,
    window_duration_minutes
from {{ ref('stg_realtime_stock_analytics') }}
where
    window_end_at <= window_start_at
    or window_duration_minutes <= 0
