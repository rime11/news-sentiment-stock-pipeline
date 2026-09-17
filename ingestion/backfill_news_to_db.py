
from datetime import datetime, date, timedelta
import duckdb
import pandas as pd
from pathlib import Path


def fill_historic_news(file):
    '''
    - connect to duckdb
    - check if table exists
    - if it doesn't create it
    '''
    current_dir = Path(__file__).resolve().parent
    db_path = current_dir.parent / 'pipeline.duckdb'
    historic_file_path = current_dir # / 'raw_news' 
    
    historic_file = pd.read_csv(historic_file_path / file)
    historic_file.shape                      
    with duckdb.connect(db_path) as conn:
     
        #create table
        conn.execute("""
                    CREATE TABLE IF NOT EXISTS news (
                        post_id VARCHAR,
                        ticker VARCHAR,
                        published_at DATETIME,
                        headline TEXT,
                        post_text TEXT,
                        vader_score DECIMAL(18,4),
                        old_sentiment_scr DECIMAL(18,4),
                        source VARCHAR,
                        url VARCHAR,
                        is_primary BOOLEAN,
                        PRIMARY KEY (post_id, ticker)
                    );
                    """)
    
       
        #insert into news table
        conn.execute('''
                    INSERT INTO news BY NAME
                    SELECT * FROM historic_file
                    ON CONFLICT (post_id, ticker) DO NOTHING;
                    ''')
        check = conn.execute('''
                    SELECT * FROM news 
                    LIMIT 10;
                    ''').fetchall()
        print(check)

fill_historic_news(current_dir /'raw_news/historic_news_data.csv')
fill_historic_news('missing_data_ready.csv')