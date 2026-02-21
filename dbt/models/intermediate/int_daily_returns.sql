{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'batch']
    )
}}

/*
    Calculates daily price returns and change metrics per symbol.
    Ephemeral — compiled inline into downstream marts, no table created.
*/

with base as (
    select * from {{ ref('stg_daily_stock_metrics') }}
),

with_returns as (
    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        batch_loaded_at,

        -- previous close for return calculations
        lag(close_price) over (
            partition by symbol
            order by trade_date
        )                                                                   as prev_close_price,

        -- daily return (%)
        round(
            (close_price - lag(close_price) over (
                partition by symbol order by trade_date
            )) / nullif(lag(close_price) over (
                partition by symbol order by trade_date
            ), 0) * 100,
            4
        )                                                                   as daily_return_pct,

        -- intraday range
        round(high_price - low_price, 4)                                    as intraday_range,
        round((high_price - low_price) / nullif(close_price, 0) * 100, 4)  as intraday_range_pct,

        -- gap from previous close to today's open
        round(
            open_price - lag(close_price) over (
                partition by symbol order by trade_date
            ),
            4
        )                                                                   as overnight_gap,

        -- row number for identifying first record per symbol
        row_number() over (
            partition by symbol order by trade_date
        )                                                                   as symbol_row_num

    from base
)

select * from with_returns
