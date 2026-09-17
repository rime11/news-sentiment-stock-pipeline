
WITH stocks AS (
    SELECT * 
    FROM {{ ref('stg_stocks') }}
),
news AS (
    SELECT * 
    FROM {{ ref('stg_news') }}
),
lookahead_intervals AS (
    SELECT UNNEST([1,3,6]) AS lookahead
)

SELECT 
    s.ticker,
    n.published_at AS news_published_at,
    n.headline,
    n.text_body,
    n.sentiment_score,
    n.old_sentiment_scr,
    n.article_source,
    n.article_url, 
    s.Day_recorded_at AS day_of_week,
    s.recorded_at AS stock_price_recorded_at,
    s.open AS open_before_news,
    s.close AS close_before_news,
    s.high AS high_before_news,
    s.low AS low_before_news,
    s.volume AS volume_before_news,
    lh.lookahead,
    DATEDIFF('minute',n.published_at + (lh.lookahead * INTERVAL '1 hours'),fs.recorded_at) AS gap_between_lookahead,
    fs.open AS open_after_news,
    fs.close AS close_after_news,
    fs.high AS high_after_news,
    fs.low AS low_after_news,
    fs.volume AS volume_after_news 

    FROM news n
    CROSS JOIN lookahead_intervals lh
    ASOF JOIN stocks s
        ON n.ticker = s.ticker
        AND s.recorded_at <= n.published_at --find stock price right before or exactly when news published
    ASOF JOIN stocks fs
        ON fs.ticker = n.ticker
        AND fs.recorded_at >= n.published_at + (lh.lookahead * INTERVAL '1 hours')
    