{{ config ( materialized ='table' ) }}

with price_movement as (
SELECT 
    ticker,
    news_published_at,
    sentiment_score,
    headline,
    article_url,
    CASE
    WHEN sentiment_score > 0.05 THEN 'positive'
    WHEN sentiment_score < -0.05 THEN 'negative'
    ELSE 'neutral'
    END AS sentiment_category,
    offset_hours, 
    gap_minutes,
    (open_at_offset - open_at_news) / NULLIF(open_at_news,0) AS open_price_movement,
    (close_at_offset - close_at_news) / NULLIF(close_at_news,0) AS close_price_movement,
    (high_at_offset - high_at_news) / NULLIF(high_at_news,0) AS high_price_movement,
    (low_at_offset - low_at_news) / NULLIF(low_at_news,0) AS low_price_movement,
    (volume_at_offset - volume_at_news) / NULLIF(volume_at_news,0) AS volume_price_movement,
    

FROM {{ ref('int_stock_news_aggregated') }})

SELECT ticker, 
    news_published_at,
    headline,
    article_url,
    sentiment_score,
    sentiment_category, 
    ROUND(open_price_movement,4) AS open_price_movement, 
    ROUND(close_price_movement,4) AS close_price_movement, 
    ROUND(high_price_movement, 4) AS high_price_movement,
    ROUND(low_price_movement,4) AS low_price_movement,
    ROUND(volume_price_movement,4) AS volume_movement,
    offset_hours,
    gap_minutes

    FROM price_movement
    
