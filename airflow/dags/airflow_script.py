import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import runpy
#from airflow.operators.bash import BashOperator
#from airflow.operators.python import PythonOperator
import duckdb
from datetime import datetime
'''
order of tasks
1. [ingest tocks, ingest news]
2. [check quality, check quality]
3. dbt build
4. export to a parquet file for streamlit
'''
#define default settings
default_args = {
    'owner': 'experiment',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
#Define the absolute directory paths inside the Docker container
@dag(
   dag_id="stock_news_ingestion_pipeline",
   default_args=default_args,
   description="a pipeline that ingests stock prices and news, transform data and join the data",
   start_date=datetime(2026,1,1),
   catchup = False,
   schedule="@daily"
)

def stock_news_pipeline():
    @task(task_id="read_stocks")
    def read_stocks():
        script_path = '/opt/airflow/ingestion/ingest_stocks.py'
        runpy.run_path(script_path, run_name='__main__')

    @task(task_id="read_news")
    def read_news():
        script_path = '/opt/airflow/ingestion/ingest_news.py'
        runpy.run_path(script_path, run_name='__main__')

    @task(task_id="stock_checks")
    #Check if ingestion silently stopped working
    def quality_stocks_checks():
        db_path = '/opt/airflow/pipeline.duckdb'
        with duckdb.connect(db_path, read_only=True) as conn:
            data_staleness, row_count = conn.execute(
                '''
                SELECT date_diff('day',MAX(CAST(Day_date AS DATE)), current_date),
                COUNT(*) 
                FROM stocks 
                ''').fetchone()
            assert row_count > 0 ,'stocks table is empty' #catches total failures
            assert data_staleness <= 7, f'stocks are {data_staleness} days old' #catches silent failures, also if max returns a Null

    @task(task_id="news_checks")
    def quality_news_checks():
        db_path = '/opt/airflow/pipeline.duckdb'
        with duckdb.connect(db_path,read_only=True) as conn:
            date_staleness, row_count = conn.execute('''
            SELECT  date_diff( 'day', MAX(CAST(published_at AS DATE)), current_date),
            COUNT(*) 
            FROM news
            ''').fetchone()
            assert row_count > 0, 'news table empty, did not ingest' #it did not fail
            assert date_staleness <=7, f'news table is {date_staleness} days old'


    @task.bash
    def run_dbt():
        return(
        'cd /opt/airflow/dbt && dbt build --profiles-dir /opt/airflow/dbt'
        )
    
    @task(task_id="export_parquet")
    def export_parquet():
        db_path = '/opt/airflow/pipeline.duckdb'
        final_path = '/opt/airflow/export/mart_stock_news.parquet'
        temp_path = '/opt/airflow/export/mart_stock_news_temp.parquet'
        with duckdb.connect(db_path, read_only = True) as conn:
             # 1. write the mart to the temp file
            conn.execute(f"COPY mart_stock_news TO '{temp_path}' (FORMAT PARQUET)")
            # 2. count rows
            mart_count = conn.execute("SELECT COUNT(*) FROM mart_stock_news").fetchone()[0]
            file_count = conn.execute(f"SELECT COUNT(*) FROM '{temp_path}'").fetchone()[0]
            # 3. check the counts match
            if file_count != mart_count:
                raise ValueError(f"Row count mismatch: mart has {mart_count} rows, but temp file has {file_count} rows")
        # 4. if they match, rename the temp file to the final file
        os.replace(temp_path, final_path) #not os.rename because it fails on windows
       
    # Sequential ingestion required — DuckDB only supports one writer at a time
    read_stocks() >> read_news() >> [quality_stocks_checks(),quality_news_checks()] >> run_dbt() >> export_parquet()

stock_news_pipeline()


   