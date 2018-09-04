## As seen on pythonprogramming.net

import bs4 as bs
import datetime as dt
import os
import pickle
import requests
import pandas as pd
import pandas_datareader.data as web
import sys
import time
from alpha_vantage.timeseries import TimeSeries

def save_sp500_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class':'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)

    with open("sp500tickers.pickle","wb") as f:
        pickle.dump(tickers,f)
    #print(tickers[:5])
    return tickers

len(old)
len(df)
len(test)
test.to_csv('test.csv')

def get_data_from_robinhood(reload_sp500=False, start=dt.datetime(2010,1,1), end=dt.datetime.today()-dt.timedelta(1)):
    if not os.path.exists('sp500tickers.pickle') and not reload_sp500:
        tickers = save_sp500_tickers()
    elif reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle",'rb') as f:
            tickers = pickle.load(f)

    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    for ticker in tickers:
        print(ticker)
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            df = web.DataReader(ticker, 'robinhood', start, end)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        elif update:
            df = web.DataReader(ticker, 'robinhood', start, end)
            df.reset_index(inplace=True)
            df['begins_at'] = pd.to_datetime(df['begins_at'])
            #df.index = pd.to_datetime(df.index)
            old = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
            #old.set_index('begins_at', inplace=True)
            old['begins_at'] = pd.to_datetime(old['begins_at'])
            df = pd.concat([old, df])
            df = test.drop_duplicates(keep='first', subset='begins_at')
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            print('Already have {}'.format(ticker))

def get_data_from_alpha(reload_sp500=False, outputsize='compact', update=True):
    if not os.path.exists('sp500tickers.pickle') and not reload_sp500:
        tickers = save_sp500_tickers()
    elif reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle",'rb') as f:
            tickers = pickle.load(f)

    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    ts = TimeSeries(key="GVR4ZPBY2FUX530Z",output_format='pandas')

    for ticker in tickers:
        print(ticker)
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            df, meta_data = ts.get_daily_adjusted(ticker, outputsize=outputsize)
            col_heads = {'6. volume':'volume', '8. split coefficient':'split_coef', '7. dividend amount':'divedend', '5. adjusted close':'adj_close', '1. open':'open', '2. high':'high', '4. close':'close',  '3. low':'low'}
            df.columns = df.columns.map(col_heads)
            df.index = pd.to_datetime(df.index)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        elif update:
            df, meta_data = ts.get_daily_adjusted(ticker, outputsize=outputsize)
            col_heads = {'6. volume':'volume', '8. split coefficient':'split_coef', '7. dividend amount':'divedend', '5. adjusted close':'adj_close', '1. open':'open', '2. high':'high', '4. close':'close',  '3. low':'low'}
            df.columns = df.columns.map(col_heads)
            df.index = pd.to_datetime(df.index)
            old = pd.read_csv('stock_dfs/{}.csv'.format(ticker), parse_dates=True)
            old.set_index('date', inplace=True)
            df = pd.concat([old,df]).drop_duplicates()
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            print('Already have {} up to date'.format(ticker))

        time.sleep(60)

def get_data_quandl(reload_sp500=False, start=dt.datetime(2010,1,1), end=dt.datetime.today()-dt.timedelta(1), api_key = None):
    pass

def compile_close_data():
    with open('sp500tickers.pickle', "rb") as f:
        tickers = pickle.load(f)

    main_df = pd.DataFrame()

    for count, ticker in enumerate(tickers):
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)

        df.rename(columns = {})

if __name__=='__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    get_data_from_robinhood(start=dt.datetime.today()-dt.timedelta(4),update=True)
