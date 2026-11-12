import streamlit as st
import pandas as pd 
import plotly.express as px
from datetime import datetime 
from pathlib import Path

PARQUET_PATH = Path(__file__).resolve().parent.parent / "export" / "mart_stock_news.parquet"

if not PARQUET_PATH.exists():
    st.error('No data yet. Run the airflow pipline first.')
    st.stop()
        

@st.cache_data
def load_data(parquet_file, mtime):
    '''Read in parquet file and return a dataframe'''
    return pd.read_parquet(parquet_file)

def sidebar_controls(df):
    '''Draw sidebar widgets once and return the user's choices.'''
    ticker = st.sidebar.selectbox('Choose a ticker', sorted(df['ticker'].unique()))
    date_range = st.sidebar.date_input(
        'Select date range',
        value=(df['news_published_at'].dt.date.min(), df['news_published_at'].dt.date.max()),
    )
    if len(date_range) != 2:
        st.info('Pick an end date.')
        st.stop()
    price_cols = [c for c in df.columns if c.endswith('_price_movement')]
    price = st.sidebar.selectbox('Price measure', price_cols)
    return ticker, date_range[0], date_range[1], price

def filter_data(df, ticker, start, end):
    '''Return rows for one ticker within the date range.'''
    return df[
        (df['ticker'] == ticker)
        & (df['news_published_at'].dt.date.between(start, end))
    ]

def summarize_by_sentiment(df, price):
    '''One row per sentiment category: article count and median movement.'''
    summary = df.groupby('sentiment_category').agg(
        article_count=('sentiment_category', 'size'),
        median_movement=(price, 'median'),
    )
    if 'neutral' in summary.index:
        neutral_median = summary.loc['neutral','median_movement']
        summary['vs_neutral'] = summary['median_movement'] - neutral_median
    else:
        summary['vs_neutral'] = float('nan')
    return summary

def timeline_chart(df, price):
    '''Each article as a dot over time, colored by sentiment.'''
    fig = px.scatter(
        df, x='news_published_at', y=price, color='sentiment_category',
        color_discrete_map={'positive': 'green', 'neutral': 'gray', 'negative': 'red'},
        hover_data=['headline'],
    )
    fig.add_hline(y=0)
    fig.update_yaxes(tickformat='.1%')
    return fig

def sentiment_box_chart(df, price, summary):
    '''Spread of price movement for each sentiment category, with neutral median as reference.'''
    fig = px.box(
        df,
        x='sentiment_category',
        y= price,
        color='sentiment_category',
        color_discrete_map={'positive': 'green', 'neutral': 'gray', 'negative': 'red'},          # same colors as the timeline
        category_orders={'sentiment_category': ['positive','neutral','negative']},    # positive, neutral, negative, same order as the cards
        points='all'
    )
    if 'neutral' in summary.index:                               # only if neutral exists
        fig.add_hline(y=summary.loc['neutral','median_movement'], line_dash='dash', annotation_text='neutral median')
    fig.update_yaxes(tickformat='.1%')
    return fig

# ---- layout ----
mtime = PARQUET_PATH.stat().st_mtime
df = load_data(PARQUET_PATH, mtime)

st.title('Does news sentiment move stock prices?')
st.caption(f'Data as of {datetime.fromtimestamp(mtime):%Y-%m-%d %H:%M}')
st.write(df.shape,df.head(), df['offset_hours'].value_counts())

ticker, start, end, price = sidebar_controls(df)
filtered = filter_data(df, ticker, start, end)
summary = summarize_by_sentiment(filtered, price)

categories = ['positive', 'neutral', 'negative']
cols = st.columns(3)

for col, category in zip(cols, categories):
    if category not in summary.index:
        st.metric(
            label=f"{category.title()} (n=0)",
            value= "No articles")
        continue
    row = summary.loc[category]
    delta = None if category == 'neutral' or pd.isna(row['vs_neutral']) else f"{row['vs_neutral']:.2%}"

    col.metric(
        label=f"{category.title()} (n={int(row['article_count'])} articles)",
        value=f"{row['median_movement']:.2%}",
        delta= delta
    )

#st.table(summary)   # temporary: replace with metric cards
st.plotly_chart(timeline_chart(filtered, price), width='stretch')
st.plotly_chart(sentiment_box_chart(filtered,price, summary),width='stretch')
st.write(df.columns.to_list())
st.write(st.write(df['gap_between_lookahead'].value_counts()))
