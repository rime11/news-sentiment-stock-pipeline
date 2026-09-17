
with news_source AS(
    SELECT 
        post_id,
        ticker,
        published_at,
        headline,
        post_text AS text_body,
        vader_score AS sentiment_score,
        old_sentiment_scr,
        source AS article_source,
        url AS article_url,
        is_primary,
        provider
    FROM {{ source('stock_n_news', 'news') }}
)
SELECT * FROM news_source
