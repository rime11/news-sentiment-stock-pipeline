
# Build log

Working notes from building the pipeline, what was tried, what broke, and why the design changed. Kept deliberately, including the dead ends.

## Software installation:
- praw: python riddit api wrapper -> uninstalled later because reddit is not used in this project
- yfinance: uses yahoo finance public api to fetch financial data
- duckdb: a high-performance, in-process SQL database management system optimized for analytics.
-dbt: a CLI tool that is used for data transformation in a data warehouse. Compiles select statements into tables and views 
- dbt-duckdb: an adapter that allows dbt core to communicate with duckdb as its computer engine
- vadersentiment: VADER (Valence Aware Dictionary and sEntiment Reasoner) is a simple, fast tool for analyzing text sentiment, specifically designed to understand emotions in social media posts. It calculates a score from -1 (very negative) to +1 (very positive) by recognizing not just words, but also capitalization, punctuation, emoticons, and slang
- streamlit: an open-source Python library that allows for creation and sharing of custom web applications for data science and machine learning projects with minimal effort. It provides an intuitive interface for building interactive dashboards and visualizations, making it easier for data scientists to communicate their findings and insights effectively.
- apachi airflow:  an open source platform designed to author, schedule, and monitor complex workflows

## Phase 1: Ingestion
### Prices (ingest_stocks.py):
- yfinance wraps the public Yahoo Finance API. Pulls hourly candles for the past 30 days across the five tickers.
- Stored in pipeline.duckdb, table stocks. First working version: 735 rows across 5 tickers
Ingestion must be idempotent: running it twice on the same day must not create duplicate rows. 
- Options considered:  
    * upsert: INSERT ... ON CONFLICT DO NOTHING against the primary key  
    * anti-join: insert only rows not already present
Went with ON CONFLICT DO NOTHING, which requires the table to declare a real primary key.

### News (ingest_news.py):
- Why not Reddit. The project was scoped as Reddit sentiment via PRAW. Reddit requires app authentication for API access and its terms make it awkward for a project intended to be public. Switched to Finnhub, a financial data provider with a REST API, ticker tagged company news, and a usable free tier.

- Why not X/Twitter. Priced out. At 100 posts × 5 tickers × 24 hours × 30 days = 360,000 reads/month × $0.005 ≈ $1,800/month.

### Finnhub company-news fields:

|Field|	Meaning|
|-----|--------|
|category|	always company for this endpoint|
|datetime|	Unix timestamp|
|headline|	article title|
|id |	provider article ID|
|image|	article image URL|
|related|	ticker|
|source	|publisher|
|summary|	longer article summary|
|url|	link to the article|

### Process:

- Loop over the five tickers, hitting /company-news?symbol=HOOD&from=...&to=...
- Score headline + summary with SentimentIntensityAnalyzer().polarity_scores(), keeping the compound score (−1 to +1)
- Write to the news table, PRIMARY KEY (post_id, ticker), ON CONFLICT DO NOTHING
- Delete rows older than 30 days so the table stays bounded (changed)
- Used requests: A thin client rather than anything heavier because the endpoint is simple 

**Before joining, confirm both tables agree on types and ticker spellings:**
```bash
sql
SELECT DISTINCT ticker FROM news ORDER BY ticker;
SELECT DISTINCT ticker FROM stocks ORDER BY ticker;
DESCRIBE news;
DESCRIBE stocks;
```
**Historical news: Alpha Vantage**
- I quickly realised that historic data is needed to produce a dashboard that makes sense.
- For backfill Alpha Vantage's News & Sentiment API returns a full year of news whereas Finnhub's free tier returns a week worth of news data

**Problem**: it returns no explicit article ID, but the news primary key is (post_id, ticker). Fix: derive a deterministic ID from the article's url plus time_published, so the same article always produces the same key across runs.

- time_published arrives as naive Eastern and parses with:

```python
pd.to_datetime(df['time_published'], format="%Y%m%dT%H%M%S")
```
Publish-time distribution (article count by hour, Eastern):
```
Hour   Count  Hour Count

0       268		12  578
1       222		13	625
2       210		14	602
3       221		15	487
4       225		16	868
5       194		17	469
6       267		18	1070
7       518		19	369
8       588		20	523
9       622		21	415
10      710		22	268
11      582		23	237
```
The ramp starts at 7am and runs to 4pm, with a spike at the 4pm close and a larger one at 6pm after-hours earnings and daily wrap ups. A large share of news lands outside market hours, which is what pushed the design toward next-session alignment rather than a wall clock offset.

## Phase 2: dbt
### Setup:
 - run `which dbt` to get the install path
 - then run `path/init news_stock_prediction` to inintialise a project called news_stock_prediction. This will create the dbt file structure
 - Inside models/ create: staging/, intermediate/, and marts/ folders

```
staging       uses raw sources    →  {{ source('source_name', 'table_name') }}
   ↓
intermediate  references models   →  {{ ref('model_name') }}
   ↓
marts         business output
```

- staging: clean data and cast. Naming convention `stg_[source]__[entity]s.sql`,  in this project: stg_stocks and stg_news. Renames raw columns to analysis names (datetime_created → published_at, vader_score → sentiment_score, etc.)
- intermediate: int_stock_news_aggregated: joins news to the price candle at publication and the candle that follows
- marts: mart_stock_news: sentiment buckets and percent price movement

**Join logic, first version**. 
* Snap each article's publish time to the candle grid. Candles land on the half hour, so: time_bucket(INTERVAL '1 hour', n.published_at, INTERVAL '30 minutes') -> DuckDB's time_bucket, with a 30-minute offset
* Self join stocks to itself three hours ahead to get the "after" candle
* Inner join first, so a working dashboard exists before handling after hours news, loosen to a left join afterwards
* Marts
Bucket sentiment: > 0.05 positive, < -0.05 negative, else neutral (VADER's conventional thresholds)
* Percent change per field: (after - before) / NULLIF(before, 0), rounded to 4 places
Testing

* Tests live in models/marts/schema.yml and run with dbt test. Inspect the output directly:

```sql
SELECT * FROM mart_stock_news;
```
**What is worth asserting:**

- VADER scores are actually being computed and a real distribution, not a column of zeros is produced
- The join produced enough rows. With a 30 minute grid there are 13 slots from 9:30 to 16:00, and the 3 hour offset means nothing after 12:30 matches, so the realistic floor is low. The point of this check is to catch a join that failed outright, so the minimum is 1.

## Phase 3: Airflow

- An Airflow DAG is a Python script that arranges tasks or nodes and their dependencies or edges into a directed acyclic graph, and dictates when and in what order they run. 
- airflow will be installed inside a docker container to reduce dependency conflicts

**Docker setup**
```
airflow/
├── .env
├── docker-compose.yaml
├── Dockerfile
├── requirements-airflow.txt
├── config/
├── dags/
├── logs/
└── plugins/
```

```bash
curl -LO 'https://airflow.apache.org/docs/apache-airflow/2.9.2/docker-compose.yaml'
```
Volumes link local folders to folders inside the running containers. Three custom mounts were added to `docker-compose.yaml` so the files in the container can reach the local project files:

**yaml**
- ../pipeline.duckdb:/opt/airflow/pipeline.duckdb
- ../news_stock_prediction_dbt:/opt/airflow/dbt
- ../ingestion:/opt/airflow/ingestion

**Installing packages:** two options:  
1. docker_compose.yml is configured to look for the special environment variable, {_PIP_ADDITIONAL_REQUIREMENTS:-}. Add _PIP_ADDITIONAL_REQUIREMENTS= to the .env file and list all the packages like duckdb, yfinance, etc... The .env file to tell Apache Airflow to install a quick list of Python packages at runtime without building a custom Dockerfile. This method slows down the image because packages are installed everytime
2. A better way is look for the following in the docker-compose.yml file: in docker-compose.yml comment image line and uncomment build line which will look for the Dockerfil in the current directory below
3. Create a requirements-airflow.txt file with the following packages: 
```
                - yfinance 
                - finnhub-python
                - dbt-duckdb 
                - duckdb 
                - vaderSentiment
```

4. Create Dockerfile
   
5. run 
      ```bash
      docker compose build
      ```
    to build the container
6. initialize airflow postgres metadata database 
      ```bash
      docker compose up airflow-init
      ```
    - which reads the docker-compose.yml,initializes the database and creates a user and starts 4 containers:
```
                        ┌──► [Container 1] Airflow Scheduler (Starts managing DAG timelines)
                        │
 docker compose up ─────┼──► [Container 2] Airflow Worker (Starts listening for tasks)
                        │
                        ├──► [Container 3] Airflow Webserver (Starts the UI portal on port 8080)
                        │
                        └──► [Container 4] Postgres/MySQL (The internal Airflow Metadata DB)
```
7. once it runs go to http://localhost:8000


      Note: there was a warning "AIRFLOW_UID not set!" to fix do this in terminal which will add user id in .env
      ```bash
      echo "AIRFLOW_UID=$(id -u)" >> .env
      ``` 
      - his makes files created inside the container owned by the host user instead of root, avoiding permission problems in the mounted volumes.

    - UI at http://localhost:8080.

## Installation Problems and Fixes
**How does airflow work:**
```
[ Airflow Scheduler ]
         │
         ▼  (1) Pushes task command
[  Redis  ]  (Acts as the holding queue)                 │
         │
         ▼  (2) Pulls task command                       │
 [ Celery Worker ] 
         │
         ├─► (3) Executes the actual code                │
         │
         ▼  (4) Writes "Success/Failure" status
[ Metadata DB / Result Backend ] (PostgreSQL/MySQL)      │
         ▲
         │  (5) Reads status to unlock next tasks -----─►│
 [ Airflow Scheduler ]
```
**Executor**: Celery is overkill
- Out of the box docker compose up starts scheduler, worker, webserver, and Postgres, with Redis as a task queue. Celery and Redis exist to distribute work across machines. This pipeline is a handful of sequential tasks on one machine, and the Celery worker was failing on a configuration issue anyway.   

**Fix**:change
- AIRFLOW__CORE__EXECUTOR: CeleryExecutor → AIRFLOW__CORE__EXECUTOR: LocalExecutor
- comment out the worker (and Redis) services
- docker compose down && docker compose up

**dbt profile inside the container**

- dbt reads ~/.dbt/profiles.yml for connection details, which does not exist inside the container. Rather than mounting the host file, put a profiles.yml inside the dbt project folder with the same contents but a container relative database path (/opt/airflow/dbt), and pass --profiles-dir in the task.

**The DAG**
- start_date=datetime(2026, 1, 1) ->fixed, not dynamic
- catchup=False —> without it, Airflow would backfill every interval from the start date
- schedule="@daily", retries=2, retry_delay=5 minutes
- ingest_stocks → ingest_news → [stock_checks, news_checks] → run_dbt

Calling stock_news_pipeline() at the bottom of the file instantiates the DAG and registers it with the scheduler.

**Why sequential ingestion of stocks and news?** Originally ingest_stocks and ingest_news ran in parallel and the DAG failed: DuckDB allows one writer at a time. A sleep on one task is a weak fix, a task can always take longer than the delay. The correct fix is to use Postgres as the processing database instead of duckdb; the fastest fix for a learning project is to run the tasks in sequence, documented as a known limitation.

**Problems found once the DAG was running**
- Read-only connections for the checks. The two quality check tasks run in parallel and both called duckdb.connect(db_path), which opens read write by default. Duckdb allows one writer or many readers, never both. Fixed with duckdb.connect(db_path, read_only=True), after which they can run concurrently.

- Row count assertions are the wrong check. assert row_count > n fails on every weekend and market holiday. The purpose of the check is to catch ingestion breaking for technical reasons like network loss, an expired API key, a changed endpoint. Better assertions:
    * data freshness: has new data arrived when it should have?
    * total row count with a message on the assert, so a failure says what went wrong
    * a gap check, to catch a large hole in the series

**The 3-hour offset was the wrong model. Several problems compounded:**
- Hardcoding 3 hours assumes the answer. Better to make the lag a variable and let the data show which window, if any, matters.
- Cross joining an array of [1, 3, 6] hours produces three rows per stock_news pair and lets all three windows be compared in one query with a gap column recording the real elapsed time.
- Even then, long weekends wreck comparability: some "3 hour" measurements span 3 hours and some span 3 days.
- fix:
    * ASOF JOIN fixes the exact match brittleness. Instead of requiring a candle at exactly published_at + 3h, it matches the closest candle satisfying an inequality (recorded_at <= published_at), so missing candles no longer silently drop rows or produce nulls.

- The redesign: next trading session. Map every article to the next session that could actually digest it, comparing daily candles:

    - Market not yet open at publish time → previous day's close vs. next open (article published on Tuesday 7am → Monday close vs. Tuesday open)
    - Market already open at publish time → same day's close vs. next day's close (article published on Friday 7am → Thursday close vs. Friday open)

This requires ingesting stocks at 1d intervals so daily candles, alongside the hourly ones.

**Phase 4: Streamlit**

- currently working on it