import requests, json, os, time
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

current_path = Path(__file__).resolve().parent

def get_news_data(ticker, start, end):
    '''
    Connects to Alpha Vantage API
    - ticker: stock ticker symbol
    - start: start date in the format YYYYMMDDTHHMM
    - end: end date in the format YYYYMMDDTHHMM
    - returns: json data of news articles for the given ticker and date range
    - the json file is saved to ray_news
    '''
    load_dotenv()
    API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
    
    #https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey=demo
    url = 'https://www.alphavantage.co/query'
   
    parameters = {
        'function': 'NEWS_SENTIMENT',
        'tickers':ticker,
        'time_from':start,
        'time_to':end,
        'sort':'EARLIEST',
        'limit':1000,
        'apikey':API_KEY
        }
    print(f'Getting news for {ticker}')
    response = requests.get(url, params=parameters)
    print(response)
    if response.status_code == 200:
        data = response.json()
       
        with open(f'raw_news/{ticker}_{start[:8]}.json','w') as f:
            json.dump(data,f)
      
    else:
        print(f'Error fetching news for {ticker}: {response.status_code}')

    time.sleep(15)
    return data

def json_to_df(file,ticker):
    '''
    Converts a JSON file of news articles into a pandas DataFrame
    '''
    with open(file,'r',encoding='utf-8') as f:
        #parse the json file into a python dict object
        data = json.load(f)
        # Alpha Vantage stores the articles array under the "feed" key
    articles = data.get('feed',[])
    temp_df = pd.DataFrame(articles)
    temp_df.insert(0,'ticker',ticker)
    
    
    return temp_df

tickers=['SOFI', 'STX', 'RDDT', 'GOOGL','HOOD']

end = datetime.today()
start = end - timedelta(days=365)

for ticker in tickers:
    data = get_news_data(ticker,'20250819T0000','20260819T0000')
    n = len(data.get('feed',[]))
    if n == 0:
        print('n=0 error = ', data['Error Message'])
    else:
        print(ticker, n, 'Capped' if n == 1000 else 'Not Capped')

#------------------------
data_df = pd.DataFrame()

for t in tickers:
    
    data_json = get_news_data(t,'20260818T0000','20260903T0000')
    articles = data_json.get('feed',[])
    print(len(articles))
    articles_df = pd.DataFrame(articles)
    articles_df['ticker'] = t
    data_df = pd.concat([data_df,articles_df], ignore_index=True)
    
data_df.to_csv(current_path / 'missing_data.csv', index = False)
#-----------------------------

#GOOGL and STX were capped so run the next lines
print(start)
windows = [
        ('20250819T0000','20251119T0000'),
        ('2025119T0000','20260219T0000'),
        ('20260219T0000','20260519T0000'),
        ('20260519T0000','20260819T0000')
        ]
for window in windows:
    data = get_news_data('STX',window[0],window[1])
    n = len(data.get('feed',[]))
    if n == 0:
        print('n=0 error = ', data['Error Message'])
    else:
        print('STX', n, 'Capped' if n == 1000 else 'Not Capped')

for window in windows:
    data = get_news_data('GOOGL',window[0],window[1])
    n = len(data.get('feed',[]))
    if n == 0:
        print('n=0 error = ', data['Error Message'])
    else:
        print('GOOGL', n, 'Capped' if n == 1000 else 'Not Capped')



'''
Since Google was the only one capped, I need to know how much missing data there is so I can pull it
- so convert the last google file to dataframe and print the max time_published'''
df_temp = json_to_df('raw_news/GOOGL_20260819.json','GOOGL')
print(max(df_temp['time_published'])) #it is 20260609 so start pulling from that date

#pull missing google data from 2026-06-10
data = get_news_data('GOOGL','20260610T0000','20260819T0000')
n = len(data.get('feed',[]))
if n == 0:
    print('n=0 error = ', data['Error Message'])
else:
    print('GOOGL', n, 'Capped' if n == 1000 else 'Not Capped')

#see what is the max date I have
df_temp = json_to_df('raw_news/GOOGL_20260610.json','GOOGL')
#print the max date
max(df_temp['time_published']) ##'20260706T134000'
#run it again for after that date

data = get_news_data('GOOGL','20260707T0000','20260819T0000')
n = len(data.get('feed',[]))
if n == 0:
    print('n=0 error = ', data['Error Message'])
else:
    print('GOOGL', n, 'Capped' if n == 1000 else 'Not Capped')
#convert to df
df_temp = json_to_df('raw_news/GOOGL_20260707.json','GOOGL')

max(df_temp['time_published']) #==> 20260728T203759

#download the rest
data = get_news_data('GOOGL','20260729T0000','20260819T0000')
n = len(data.get('feed',[]))
if n == 0:
    print('n=0 error = ', data['Error Message'])
else:
    print('GOOGL', n, 'Capped' if n == 1000 else 'Not Capped')
df_temp = json_to_df('raw_news/GOOGL_20260729.json','GOOGL')

max(df_temp['time_published'])#==>20260818T
pd.concat([data_df,data],axis=1)

