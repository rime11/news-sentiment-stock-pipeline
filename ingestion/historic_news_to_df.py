import hashlib
import json
import pandas as pd
from pathlib import Path
import datetime as dt
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

current_dir = Path(__file__).resolve().parent

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
    #add a column for the ticker as first column
    temp_df.insert(0,'ticker',ticker)

    return temp_df

def df_with_all_tickers(all_tickers_list):
    '''
    Given a list of tuples (json_file_path, ticker), convert each JSON file to a DataFrame and concatenate them into a single DataFrame.
    '''
    all_data_df=  json_to_df(all_tickers_list[0][0],all_tickers_list[0][1])
    
    for json_data in all_tickers_list[1:]:
        
        temp_df = json_to_df(json_data[0],json_data[1])
        all_data_df = pd.concat([all_data_df,temp_df],ignore_index=True)
        print(all_data_df.shape)
    return all_data_df

def add_missing_columns(df):

    df= df.copy()

    #add vader score
    analyser = SentimentIntensityAnalyzer()

    df['post_id']= (df['url']+'_'+df['time_published'].astype(str)).apply(lambda x:hashlib.md5(x.encode('utf-8')).hexdigest())
    df['published_at'] = pd.to_datetime(df['time_published'],format="%Y%m%dT%H%M%S")
    #calculate vader score
    text = df['title'].fillna('') + ' ' + df['summary'].fillna('')
    df['vader_score'] = text.apply(lambda t: analyser.polarity_scores(t)['compound'])

    def old_scrs(row):
         '''
            Given a row of the dataframe and a ticker, return old dentiment score and True if the ticker is the one 
            with the highest relevance score in the ticker_sentiment list, otherwise return False.
            Sice google has two tickers that can appear in news, GOOG and GOOGL, a list will be passed as tickers
         '''
         tickers = ['GOOG','GOOGL'] if row['ticker'] == 'GOOGL' else [row['ticker']]
         both_scrs = [(float(t['ticker_sentiment_score']),float(t['relevance_score'])) 
                         for t in row['ticker_sentiment'] 
                         if t['ticker'] in tickers]
         if not both_scrs:
             return None, False
        #unpack the tuple
         old_sentiment_scr, relevance_scr = zip(*both_scrs)

         max_rel = max(float(t['relevance_score']) for t in row['ticker_sentiment'])

         return max(old_sentiment_scr), max(relevance_scr) == max_rel
    #apply the function
    df[['old_sentiment_scr','is_primary']] = df.apply(old_scrs, axis=1, result_type='expand')

    #rename some columns
    df = df.rename(columns={'title':'headline','summary':'post_text'})
                
    return df[['post_id','ticker','published_at','headline','post_text',
               'vader_score','old_sentiment_scr','source','url','is_primary']]
     
'''
missing_data_df = pd.read_csv(current_dir / 'missing_data.csv')

tickers=['SOFI', 'STX', 'RDDT', 'GOOGL','HOOD']
sofi_df = missing_data_df.iloc[0:31].copy()
sofi_df['ticker'] = 'SOFI'
stx_df = missing_data_df.iloc[31:117].copy()
stx_df['ticker'] = 'STX'
rddt_df = missing_data_df.iloc[117:128].copy()
rddt_df['ticker']= 'RDDT'
googl_df = missing_data_df.iloc[128:].copy()
googl_df['ticker'] = 'GOOGL'

all_dfs = pd.concat([sofi_df,stx_df,rddt_df,googl_df],ignore_index=True)
import ast
all_dfs['ticker_sentiment'] = all_dfs['ticker_sentiment'].apply(ast.literal_eval)

all_dfs.iloc[500]['ticker_sentiment']
all_tickers_df.iloc[100]['ticker_sentiment']



all_cols_df = add_missing_columns(all_dfs)
all_cols_df
all_cols_df.to_csv(current_dir / 'missing_data_ready.csv',index=False)


Getting news for SOFI
<Response [200]>
31
Getting news for STX
<Response [200]>
86
Getting news for RDDT
<Response [200]>
11
Getting news for GOOGL
<Response [200]>
522
Getting news for HOOD
<Response [200]>
0
'''


json_file_list=[
    ('raw_news/GOOGL_20250819.json','GOOGL' ),('raw_news/GOOGL_2025119T.json','GOOGL' ),
    ('raw_news/GOOGL_20260219.json','GOOGL' ),('raw_news/GOOGL_20260519.json','GOOGL' ),
    ('raw_news/GOOGL_20260610.json','GOOGL' ),('raw_news/GOOGL_20260707.json','GOOGL' ),
    ('raw_news/GOOGL_20260729.json','GOOGL' ),('raw_news/HOOD_20250819.json','HOOD' ),
    ('raw_news/SOFI_20250819.json','SOFI' ),('raw_news/STX_20250819.json','STX'),
    ('raw_news/STX_2025119T.json','STX'),('raw_news/STX_20260219.json','STX'),
    ('raw_news/STX_20260519.json','STX'),('raw_news/RDDT_20250819.json','RDDT')
    ]

all_tickers_df = df_with_all_tickers(json_file_list)

all_tickers_df.shape #(11138, 14)
all_tickers_df.info() #banner_image has nulls
all_tickers_df.isna().sum()
all_tickers_df.columns
'''
['ticker', 'title', 'url', 'time_published', 'authors', 'summary',
       'banner_image', 'source', 'category_within_source', 'source_domain',
       'topics', 'overall_sentiment_score', 'overall_sentiment_label',
       'ticker_sentiment']
'''

#apply add missing columns
df_with_all_columns = add_missing_columns(all_tickers_df)
df_with_all_columns.columns
'''['post_id', 'ticker', 'published_at', 'headline', 'post_text',
       'vader_score', 'old_sentiment_scr', 'source', 'url', 'is_primary']'''

df_with_all_columns['is_primary'].value_counts()
'''
is_primary
False    5982
True     5156
'''

df_with_all_columns[df_with_all_columns['ticker']=='GOOGL']['is_primary'].value_counts()
df_with_all_columns[df_with_all_columns['ticker']=='STX']['is_primary'].value_counts()

#save the df
df_with_all_columns.to_csv(current_dir / 'raw_news/historic_news_data.csv',index=False)
#checks 
df = pd.read_csv('raw_news/historic_news_data.csv')
df.shape
df.duplicated(subset=['post_id','ticker']).sum() #there are 50 duplicated