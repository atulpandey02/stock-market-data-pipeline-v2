{{
    config(
        materialized='view',
        tags=['staging', 'streaming']
    )
}}

/*
    Staging model for real-time streaming stock analytics.
    Source: STOCKMARKETSTREAM.PUBLIC.REALTIME_STOCK_ANALYTICS
    Loaded by: src/snowflake/scripts/load_stream_to_snowflake.py

    Responsibilities:
    - Rename columns to snake_case standard
    - Cast data types explicitly
    - Derive window duration metadata
    - Filter incomplete window records
*/

with source as (
    select * from {{ source('snowflake_stream', 'REALTIME_STOCK_ANALYTICS') }}
),

renamed as (
    select
        -- identifiers
        upper(trim(SYMBOL))                         as symbol,

        -- window timing
        cast(WINDOW_START as timestamp_ntz)         as window_start_at,
        cast(WINDOW_END as timestamp_ntz)           as window_end_at,
        datediff(
            'minute',
            cast(WINDOW_START as timestamp_ntz),
            cast(WINDOW_END as timestamp_ntz)
        )                                            as window_duration_minutes,

        -- moving averages
        cast(MA_15M as float)                       as ma_15m,
        cast(MA_1H as float)                        as ma_1h,

        -- volatility
        cast(VOLATILITY_15M as float)               as volatility_15m,

        -- volume
        cast(VOLUME_SUM_15M as bigint)              as volume_15m,

        -- metadata
        cast(LOAD_TIMESTAMP as timestamp_ntz)       as stream_loaded_at,
        current_timestamp()                          as dbt_updated_at

    from source
),

cleaned as (
    select *
    from renamed
    where
        symbol is not null
        and window_start_at is not null
        and window_end_at is not null
        and window_start_at < window_end_at
        -- only include data from the last 90 days for performance
        and window_start_at >= dateadd('day', -90, current_timestamp())
)

select * from cleaned
