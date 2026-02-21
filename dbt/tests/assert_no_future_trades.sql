-- Test: No trade dates should be in the future
-- Fails if any rows are returned

select
    symbol,
    trade_date
from {{ ref('stg_daily_stock_metrics') }}
where trade_date > current_date()
