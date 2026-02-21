-- Test: High price must never be below low price on any trading day
-- Fails if any rows are returned

select
    symbol,
    trade_date,
    high_price,
    low_price
from {{ ref('stg_daily_stock_metrics') }}
where high_price < low_price
