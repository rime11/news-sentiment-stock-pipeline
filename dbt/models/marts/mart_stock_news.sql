{{ config (
    materialized ='external',
    location ='../export/mart_stock_news.parquet',

) }}

with price_movement as (
SELECT 
    ticker,
    news_published_at,
    sentiment_score,
    CASE
    WHEN sentiment_score > 0.05 THEN 'positive'
    WHEN sentiment_score < -0.05 THEN 'negative'
    ELSE 'neutral'
    END AS sentiment_category, 

    (open_after_news - open_before_news) / NULLIF(open_before_news,0) AS open_price_movement,
    (close_after_news - close_before_news) / NULLIF(close_before_news,0) AS close_price_movement,
    (high_after_news - high_before_news) / NULLIF(high_before_news,0) AS high_price_movement,
    (low_after_news - low_before_news) / NULLIF(low_before_news,0) AS low_price_movement,
    (volume_after_news - volume_before_news) / NULLIF(volume_before_news,0) AS volume_price_movement

FROM {{ ref('int_stock_news_aggregated') }})

SELECT ticker, 
    news_published_at,
    sentiment_score,
    sentiment_category, 
    ROUND(open_price_movement,4) AS open_price_movement, 
    ROUND(close_price_movement,4) AS close_price_movement, 
    ROUND(high_price_movement, 4) AS high_price_movement,
    ROUND(low_price_movement,4) AS low_price_movement,
    ROUND(volume_price_movement,4) AS volume_movement

    FROM price_movement
    
