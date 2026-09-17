
with stock_source as (
    select
        Day_of_week AS Day_recorded_at,
        Day_date AS recorded_at,
        Ticker AS ticker,
        Open AS open,
        Close AS close,
        High AS high,
        Low AS low,
        CAST(Volume AS BIGINT) AS volume
    from {{ source('stock_n_news', 'stocks') }}
)

SELECT * FROM stock_source

