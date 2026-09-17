

<center><h1> News Sentiment vs. Stock Price</h1></center>
    Stack: Python → DuckDB → dbt → Airflow (Docker) → Streamlit Tickers: HOOD, SOFI, STX, RDDT, GOOGL

## Description
    An end-to-end data pipeline that ingests financial news and intraday stock prices for five tickers, scores each article's sentiment, aligns news to the price action that follows it, and surfaces the result on a dashboard.

## The question
    Started with the hypothesis: does sentiment lead price by three hours? However, building it revealed that the original hypothesis was narrow. The reason found was that a fixed 3-hour offset silently compares very different things, 3 hours after a 10am article is the same trading day, 3 hours after a 5pm Friday article is the following Monday. So the question was changed to: 
**Does the sentiment of a news article_ move a stock's price and if so, how long does it take?**
    
    The current design instead maps every article to the next trading session that could actually react to it, and treats the lag itself as something to measure rather than assume.

## Status:
    Ingestion, dbt models, and Airflow orchestration run end to end. The serving layer (Parquet export → Streamlit) and the move from a fixed 3-hour offset to trading-session alignment are in progress. See Roadmap.

# Architecture
```text
Finnhub / Alpha Vantage (news) ──┐
                                 ├──► Python ingestion ──► DuckDB (raw layer)
Yahoo Finance (prices) ──────────┘                              │
                                                                ▼
                                          dbt (staging → intermediate → marts)
                                                                │
                                          Airflow DAG (daily, Docker, LocalExecutor)
                                                                │
                                                  Parquet export (serving layer)
                                                                │
                                                                ▼
                                                     Streamlit dashboard
```
Processing storage (DuckDB) is deliberately separated from serving storage (Parquet). DuckDB allows one writer or many readers, never both, so a dashboard reading the database while Airflow writes to it will lock. Airflow writes, then exports a Parquet snapshot and Streamlit only ever reads the snapshot. The dashboard shows data as of the last successful run, and a failed run leaves the last good snapshot in place instead of breaking the app.

# Stack

|Tool       |	Role    |
|:-----------|:-----------|
|yfinance |	Yahoo Finance price data (OHLCV candles)| 
|Finnhub  |	Company news — free tier, ~1 year of history |
|Alpha Vantage |	News & Sentiment API, used for deeper historical backfill |
|VADER |	Rule-based sentiment scoring (−1 to +1 compound score); tuned for social/headline text — reads capitalization, punctuation, emoji, and slang
|DuckDB	| In-process analytical database; the raw and transformed layers|
|dbt (dbt-duckdb)|	SQL transformations, model lineage, and data tests |
|Airflow |	Orchestration and scheduling, running in Docker |
|Streamlit |	Dashboard|

# Repo layout
```text
news_stock_pipeline/    
├── pipeline.duckdb                  # database (gitignored)
├── ingestion/
|   ├──backfill_news_to_df.py        # combines the json files from Alpha Vantage to dataframe
|   ├──backfill_news_to_db.py        # uploads historic last year's data to the database
│   ├── ingest_stocks.py             # yfinance → stocks table
│   └── ingest_news.py               # Finnhub + VADER → news table
├── dbt/                             # dbt project
│   ├── profiles.yml                 # container-relative paths
│   └── models/
│       ├── staging/                 # stg_stocks.sql, stg_news.sql, sources.yml
│       ├── intermediate/            # int_stock_news_aggregated.sql
│       ├── marts/                   # mart_stock_news.sql, schema.yml
│       └── export/                  # export_mart_stock_news.sql
├── airflow/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── requirements-airflow.txt
│   └── dags/airflow_script.py       # dag_id: stock_news_ingestion_pipeline
├── dashboard/                       # Streamlit app
├── docs/BUILD_LOG.md                # design decisions and build notes
├── requirements.txt
└── .env                             # API keys (gitignored)
```
# Quickstart
1. Install
```bash 
python -m venv news_stock_pipeline && source news_stock_pipeline/bin/activate
python -m pip install -r requirements.txt
#or with conda
conda create -n news_stock_pipeline python=3.11
conda activate news_stock_pipeline
python -m pip install -r requirements.txt
```
2. Configure

    Create a .env in the project root:
```bash
FINNHUB_API_KEY=your_key_here
ALPHAVANTAGE_API_KEY=your_key_here
```
Both providers have free tiers. Keys are never committed, .env is gitignored.

3. Ingest
```bash
python ingestion/ingest_stocks.py   # ~30 days of candles for 5 tickers
python ingestion/ingest_news.py     # company news + VADER scores
```
Both scripts are idempotent: re-running them on the same day does not duplicate rows because of (INSERT ... ON CONFLICT DO NOTHING on the table's primary key).

 Inspect the result:

```bash
duckdb pipeline.duckdb
#sql
SHOW TABLES;
SELECT ticker, COUNT(*) FROM news GROUP BY ticker;
SELECT MIN(datetime_created), MAX(datetime_created) FROM news;
```
4. Transform
```bash
cd dbt
dbt run
dbt test
```
5. Orchestrate (airflow)
    Build a docker container to run airflow out of(make sure Docker Desktop is installed)
```bash
cd airflow
echo "AIRFLOW_UID=$(id -u)" >> .env    # avoids root owned files in mounted volumes
docker compose build                   # build container
docker compose up airflow-init         # initializes the Postgres metadata DB
docker compose up
```
Airflow UI: http://localhost:8080. The DAG stock_news_ingestion_pipeline runs @daily.

6. Dashboard
Currently building 
```bash
streamlit run dashboard/app.py
```
# Data Model
Raw layer written by ingestion scripts:

**stocks**:  OHLCV candles from yfinance, keyed on (ticker, date_time).

**news**: one row per article per ticker:

|Column	|Type	|Notes|
|-------|-------|------|
|post_id|VARCHAR	|Provider article ID, part of the primary key|
|ticker	|VARCHAR	|Part of the primary key|
|datetime_created|	TIMESTAMP|	Publish time, naive US/Eastern|
|headline|	TEXT|	
|post_text|	TEXT|	Article summary|
|vader_score| DECIMAL(18,4)|VADER compound score over headline + summary|
|source|VARCHAR|Publisher|
|url|	VARCHAR

At the beginning rows older than 30 days are deleted on each run to keep the table bounded but soon realized that data is needed to create a dashboard so this was removed

# dbt layers
|Layer|	Model|	Purpose|
|-----|------|---------|
|staging|	stg_stocks, stg_news|	Clean, rename, cast. Sources referenced with {{ source() }}|
|intermediate|	int_stock_news_aggregated|	Joins each article to the price candle at publication and the candle that follows it|
|marts|	mart_stock_news|	Sentiment buckets (positive / neutral / negative at ±0.05) and percent price movement per OHLCV field|
|export|	export_mart_stock_news|	Writes the mart to Parquet for the dashboard to read|

Tests live in models/marts/schema.yml and assert the things that would silently break the analysis: that the join produced rows at all, that sentiment scores fall in [-1, 1] with a real distribution rather than a wall of zeros, and that the mart is not stale.

# Engineering decisions

These are the trade offs and the changes. Longer write ups are in docs/BUILD_LOG.md.

**Reddit** → news APIs. The project started on Reddit via PRAW. Reddit's API terms and authentication made it unworkable for a public portfolio project, and X/Twitter priced out at roughly $1,800/month for the volume this needs (100 posts × 5 tickers × 24 hours × 30 days ≈ 360k reads). Finnhub's company news endpoint gives clean, ticker-tagged, timestamped text on a free tier; Alpha Vantage covers historical backfill.

**Idempotent ingestion** Airflow re-runs and manual re-runs must not duplicate rows, so both tables, news and stocks carry a primary key and inserts use ON CONFLICT DO NOTHING. Alpha Vantage returns no article ID, so one is derived deterministically from url + time_published — the same article always hashes to the same key. This is repeated for the Finnhub data so all the data matches. 

**DuckDB has one writer at a time** Honestly, it would have been easier to use Postgres but dropped because it exceeded the scope of this learning project. Since DuckDB permits a single writer or multiple readers, the two ingestion tasks (read_news and read stocks that write to duckDB and quality checks reads from database) could not run in parallel, they now run in sequence and the quality check tasks now open duckdb.connect(db_path, read_only=True) so they can run concurrently.

**Freshness checks, not row counts** assert row_count > n fails every weekend and market holiday, which trains you to ignore the alert. The check that matters is whether ingestion is stale, whether new data arrived when it should have, plus a gap check on the join.

**ASOF joins instead of exact timestamp matching** Candles land on the half hour, articles do not. Bucketing publish times to the nearest 30 minutes works until a candle is missing and the row silently disappears. DuckDB's ASOF JOIN matches each article to the closest preceding candle, which is both more accurate and null safe.

**Trading sessions, not actual time offsets** The biggest correction to the original design. Roughly a third of articles publish outside market hours, and the volume histogram shows a ramp from 7am, a spike at the 4pm close, and a second spike around 6pm for after hours earnings. A fixed 3 hour offset compares a same day intraday move against a three day weekend gap. Each article is now mapped to the next session that could react to it, pre open news and previous close → next open, intraday news and same close → next close. The lag window (1h / 3h / 6h / next session) is a parameter that gets measured rather than a constant that gets assumed.

Roadmap
 Phase 1 — Ingestion: historical and current prices and news landing in DuckDB, idempotent
 Phase 2 — dbt: staging → intermediate → marts, with tests
 Phase 3 — Airflow: DAG running in Docker with quality gates
 Phase 4 — Serving layer: Parquet export + Streamlit dashboard
 Rework the join to trading-session alignment with daily candles
 Compare lag windows (1h / 3h / 6h / next session) rather than fixing one
 Publish findings

Findings

To be filled in once the session-aligned join is running. The intended output is a comparison of percent price movement by sentiment bucket across lag windows, per ticker — including the null result, if that is what the data says.