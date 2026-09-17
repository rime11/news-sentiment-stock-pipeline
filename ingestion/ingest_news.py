import requests, os, json
from dotenv import load_dotenv
import finnhub
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
import duckdb, hashlib
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from pathlib import Path

load_dotenv()

def update_news_table():
    '''
    - connect to duckdb
    - check if table exists
    - if it doesn't create it
    - pull data from finnhub for 1 month and insert into table using read_finnhub function
    '''
    current_dir = Path(__file__).resolve().parent
    db_path = current_dir.parent / 'pipeline.duckdb'
 
    with duckdb.connect(db_path) as conn:
        #call read_finnhub function that returns a dataframe
        data_df = read_finnhub()
        
        conn.execute('''
                    INSERT INTO news BY NAME
                    SELECT * FROM data_df
                    ON CONFLICT (post_id, ticker) DO NOTHING;
                    ''')

def read_finnhub(tickers = ['HOOD', 'SOFI', 'STX', 'RDDT', 'GOOGL'],days_back=7):
    '''
    - pull news data from finnhub for the last month for the given tickers
    - use vader sentiment analysis to calculate the sentiment score for each article
    - return a dataframe with the following columns: post_id, ticker, published_at, headline, post_text, vader_score, source, url
    '''

    analyser = SentimentIntensityAnalyzer()
    today = date.today()
    start_date = today - timedelta(days=days_back)
  
    ET = ZoneInfo('America/New_York')
    api_token = os.getenv("FINNHUB_API_KEY")
    if not api_token:
        print("Error: FINNHUB_API_KEY environment variable not found.")
        return pd.DataFrame()
    
    finnhub_client = finnhub.Client(api_key=api_token)
    data_dict = []
    
        #attempt api call
    for ticker in tickers:
        try:
            #Querying string dates format 'YYYY-MM-DD'
            news_data = finnhub_client.company_news(ticker, _from=start_date.strftime('%Y-%m-%d'), to=today.strftime('%Y-%m-%d'))
           
        except finnhub.FinnhubRequestException as e:
        # This catches specific Finnhub API errors (e.g., Invalid API key, Rate limit exceeded)
            print(f"Finnhub API Error for {ticker}, Message: {e.message}")
            continue #go next ticker    
        
        for article in news_data:
            article_date = article.get('datetime')
            eastern_date = datetime.fromtimestamp(article_date, tz=timezone.utc)\
                                                                            .astimezone(ET)\
                                                                            .strftime('%Y-%m-%d %H:%M:%S')
            text_to_score = article.get('headline','')+' '+article.get('summary','') 
            score = analyser.polarity_scores(text_to_score)

            row={
                'post_id': hashlib.md5(f"{article['url']}_{eastern_date}".encode('utf-8')).hexdigest(),
                'published_at' : eastern_date, 
                'ticker' : ticker,
                'headline' : article.get('headline'),
                'post_text' : article.get('summary'),
                #vader score
                'vader_score' : score['compound'],
                'source' : article.get('source'),
                'url' : article.get('url'),
                'is_primary':True,
                'provider':'finnhub'
                }
        
            data_dict.append(row)

        #data_df = pd.DataFrame()
    data_df = pd.DataFrame(data_dict)
        
    return data_df

data = read_finnhub()
#data['published_at'].min(),data['published_at'].max()

'''
#test data
current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir,'..','pipeline.duckdb')
  

with duckdb.connect(db_path) as conn:
    print(conn.execute("show tables;").fetchall())
    print(conn.execute("SELECT * FROM news LIMIT 5").fetchdf())
    #print(conn.execute("DESCRIBE news").fetchdf())
    print(conn.execute("SELECT DISTINCT ticker FROM news ORDER BY ticker;").fetchdf())
    #print(conn.execute("select count(*) from news;").fetchone())
    #duplicates
    print(conn.execute("""
                SELECT post_id, ticker, COUNT(*)
                FROM news
                GROUP BY post_id, ticker
                HAVING COUNT(*) > 1;
                """).fetchdf())

    print(conn.execute("select ticker,count(*) from news group by ticker;").fetchdf())

    print(conn.execute("select min(published_at), max(published_at)from news;").fetchdf())

    print(conn.execute('SELECT max(vader_score), min(vader_score), avg(vader_score) FROM news;').fetchone())


'''
if __name__ == '__main__':
    update_news_table()

