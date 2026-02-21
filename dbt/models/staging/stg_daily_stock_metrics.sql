{{
    config(
        materialized='view',
        tags=['staging', 'batch']
    )
}}

/*
    Staging model for daily historical stock metrics.
    Source: STOCKMARKETBATCH.PUBLIC.DAILY_STOCK_METRICS
    Loaded by: src/snowflake/scripts/load_to_snowflake.py

    Responsibilities:
    - Rename columns to snake_case standard
    - Cast data types explicitly
    - Filter out obviously bad records (nulls, zero prices)
    - Add metadata columns for lineage
*/

with source as (
    select * from {{ source('snowflake_batch', 'DAILY_STOCK_METRICS') }}
),

renamed as (
    select
        -- identifiers
        upper(trim(SYMBOL))                         as symbol,

        -- dates
        cast(DATE as date)                          as trade_date,

        -- prices
        cast(DAILY_OPEN as float)                   as open_price,
        cast(DAILY_HIGH as float)                   as high_price,
        cast(DAILY_LOW as float)                    as low_price,
        cast(DAILY_CLOSE as float)                  as close_price,

        -- volume
        cast(DAILY_VOLUME as bigint)                as volume,

        -- metadata
        cast(BATCH_LOAD_TIMESTAMP as timestamp_ntz) as batch_loaded_at,
        current_timestamp()                          as dbt_updated_at

    from source
),

cleaned as (
    select *
    from renamed
    where
        symbol is not null
        and trade_date is not null
        and close_price is not null
        and close_price > 0
        and volume >= 0
        -- exclude future-dated records
        and trade_date <= current_date()
)

select * from cleaned
