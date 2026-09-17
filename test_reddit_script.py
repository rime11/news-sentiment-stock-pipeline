
#imports
#load env variables
#connect to duckdb
# create table if not exists called news
#initialize vader
#connect to finnhub
#for ticker in tickers, download articles for a month:
    #for article in articles:
        #save id, ticker, datetime(convert to datetime),headline, summary, vader_score = calculate score, source, url
        #insert into news, on conflict(id, ticker) do nothing 

import requests
from dotenv import load_dotenv
import datetime
from datetime import date, timedelta
import os 
import json
import duckdb
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd

load_dotenv()

def connect_db():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir,'pipeline.duckdb')
    with duckdb.connect(db_path) as conn:
        
        conn.execute("""
                    CREATE TABLE IF NOT EXISTS news (
                        post_id VARCHAR,
                        ticker VARCHAR,
                        datetime_created DATETIME,
                        headline TEXT,
                        post_text TEXT,
                        vader_score DECIMAL(18,4),
                        source VARCHAR,
                        url VARCHAR,
                        PRIMARY KEY (post_id, ticker)
                    );
                    """)
    
        data_df = read_finnhub()

        
        conn.execute("""
                    INSERT INTO news
                    SELECT * FROM data_df
                    ON CONFLICT (post_id, ticker) DO NOTHING;
                    """
        )


def read_finnhub(tickers = ['HOOD', 'SOFI', 'STX', 'RDDT', 'GOOGL']):
    tickers = tickers
    analyser = SentimentIntensityAnalyzer()
    today = date.today()
    month_ago = today - timedelta(days=30)
    api_token = os.getenv("FINNHUB_API_KEY")
    url = f"https://finnhub.io/api/v1/company-news/"

    data_dict = []

    for ticker in tickers:
        params = {
                "symbol":ticker,
                "from":month_ago,
                "to":today,
                "token":api_token
            }
        
        response = requests.get(url, params=params)

        if response.status_code == 200:
            articles = response.json()
            
        
            for article in articles:
                article_date = article['datetime']
                score = analyser.polarity_scores(article['headline']+article['summary'])

                row={
                'post_id': str(article['id']),
                'ticker' : article['related'],
                'datetime_created' : datetime.datetime.fromtimestamp(article_date, datetime.timezone.utc),
                'headline' : article['headline'],
                'post_text' : article['summary'],
                #vader score
                'vader_score' : score['compound'],
                'source' : article['source'],
                'url' : article['url']
                }
                data_dict.append(row)
        else:
            print(f"Error: {response.status_code} - {response.text}")

    data_df = pd.DataFrame(data_dict)

    return data_df

current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir,'pipeline.duckdb')
with duckdb.connect(db_path) as conn:
    data = conn.execute('select count(*) from news;').fetchall()
    count = conn.execute("select ticker, count(*) from information_schema.columns where table_name = 'news';").fetchone()
    print(count, data)

if __name__ == '__main__':
    connect_db()

