
WITH stocks AS (
    SELECT * 
    FROM {{ ref('stg_stocks') }}
),
news AS (
    SELECT * 
    FROM {{ ref('stg_news') }}
),
offsets AS (
    SELECT UNNEST([-6, -3, -1, 1, 3, 6]) AS offset_hours
),
news_after AS (
    SELECT
        n.ticker,
        n.published_at AS news_published_at,
        n.headline,
        n.text_body,
        n.sentiment_score,
        n.old_sentiment_scr,
        n.article_source,
        n.article_url,
        s.Day_recorded_at AS day_of_week,
        s.recorded_at AS stock_price_recorded_at,
        s.open AS open_at_news,
        s.close AS close_at_news,
        s.high AS high_at_news,
        s.low AS low_at_news,
        s.volume AS volume_at_news,
        o.offset_hours,
        DATEDIFF('minute', n.published_at + o.offset_hours * INTERVAL '1 hour', fs.recorded_at) AS gap_minutes,
        fs.open AS open_at_offset,
        fs.close AS close_at_offset,
        fs.high AS high_at_offset,
        fs.low AS low_at_offset,
        fs.volume AS volume_at_offset
    FROM news n
    CROSS JOIN offsets o
    ASOF JOIN stocks s
        ON n.ticker = s.ticker
        AND s.recorded_at <= n.published_at
    ASOF JOIN stocks fs
        ON fs.ticker = n.ticker
        AND fs.recorded_at >= n.published_at + o.offset_hours * INTERVAL '1 hour'
    WHERE o.offset_hours > 0
),

news_before AS (
    SELECT
        n.ticker,
        n.published_at AS news_published_at,
        n.headline,
        n.text_body,
        n.sentiment_score,
        n.old_sentiment_scr,
        n.article_source,
        n.article_url,
        s.Day_recorded_at AS day_of_week,
        s.recorded_at AS stock_price_recorded_at,
        s.open AS open_at_news,
        s.close AS close_at_news,
        s.high AS high_at_news,
        s.low AS low_at_news,
        s.volume AS volume_at_news,
        o.offset_hours,
        DATEDIFF('minute', n.published_at + o.offset_hours * INTERVAL '1 hour', fs.recorded_at) AS gap_minutes,
        fs.open AS open_at_offset,
        fs.close AS close_at_offset,
        fs.high AS high_at_offset,
        fs.low AS low_at_offset,
        fs.volume AS volume_at_offset
    FROM news n
    CROSS JOIN offsets o
    ASOF JOIN stocks s
        ON n.ticker = s.ticker
        AND s.recorded_at <= n.published_at
    ASOF JOIN stocks fs
        ON fs.ticker = n.ticker
        AND fs.recorded_at <= n.published_at + o.offset_hours * INTERVAL '1 hour'
    WHERE o.offset_hours < 0
)

SELECT * 
FROM news_after
UNION ALL 
SELECT * 
FROM news_before
