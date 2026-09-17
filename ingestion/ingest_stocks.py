#connect to yfinance api
#pull the data needed
#dump into duckdb
#insert in a table but make sure no duplicates


import yfinance as yf
import duckdb
import pandas as pd
import os

def ingest_stocks():
    '''
    - connect to duckdb
    - check if table exists
    - if it doesn't create it
    - pull data from yfinance for 1 year and insert into table
    - if table exists check if it is empty
    - if it is download and insert the data for 1 year
    - if it is not empty download 1 day and insert
    '''

    #connect to the duckdb database called: pipline.duckdb
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, "..","pipeline.duckdb")
    
    with duckdb.connect(db_path) as con:
        tbl = con.execute("""SELECT * FROM duckdb_tables() WHERE table_name = 'stocks';""").fetchone()

        if tbl is None:
            #create the data to a table called: stocks with primary keys date and ticker
            con.execute("""
                        CREATE TABLE IF NOT EXISTS stocks  (
                        Day_date DATE,
                        Day_of_week VARCHAR,
                        Ticker VARCHAR,
                        Close DECIMAL(18,4),
                        High DECIMAL(18,4),
                        Low DECIMAL(18,4),
                        Open DECIMAL(18,4),
                        Volume DECIMAL(18,4),
                        PRIMARY KEY (Ticker, Day_date) 
                        );
                        """)       
        #check if table is empty then download data for a year and insert in created table
        if con.execute('select count(*) from stocks').fetchone()[0] == 0:
            data_df = download_data('1y')
            
            con.execute('''
                    INSERT INTO stocks by NAME
                    SELECT * FROM data_df
                    ON CONFLICT (Ticker, Day_date) DO NOTHING ;
                    ''')
        #if table is not empty then add the last day    
        else: 
            data_df = download_data('1d')
        
            con.execute('''
                    INSERT INTO stocks by NAME
                    SELECT * FROM data_df
                    ON CONFLICT (Ticker, Day_date) DO NOTHING ;
                    ''')
                
def download_data(period):
    '''
    Download stock prices from yfinance for the 5 tickers
    Return a dataframe that is indexed with the Day_date, ticker and their prices
    '''
    ticker_list = ['HOOD', 'SOFI', 'STX', 'RDDT', 'GOOGL']

    
    data = yf.download(tickers= ticker_list, period = period, interval = '1d',ignore_tz = True, group_by="ticker") 

    #stack ticker to index
    data_stacked = data.stack(level=0)

    #Take out column name
    data_stacked.columns.name = None
    
    #reset index
    data_df = data_stacked.reset_index()
    #rename Datetime to Day_date which is the name of column in database
    data_df['Day_of_week'] = data_df['Date'].dt.day_name()
    data_df.rename(columns = {'Date':'Day_date'}, inplace = True)
    
    return data_df

'''
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, "..","pipeline.duckdb")
    
    with duckdb.connect(db_path) as con:
        print(con.execute("SHOW TABLES;").fetchall())
        print(con.execute("SELECT DISTINCT ticker FROM stocks ORDER BY ticker").fetchdf())
        print(con.execute("DESCRIBE stocks;").fetchdf())
        print(con.execute("""SELECT Ticker, count(*) FROM stocks GROUP BY Ticker;""").fetchall())
        print(con.execute("""SELECT * from stocks limit 3;""").fetchdf())
        '''

if __name__ == '__main__':
    ingest_stocks()

