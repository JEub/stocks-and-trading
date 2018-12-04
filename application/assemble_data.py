import pandas_datareader.data as web
import pandas_datareader.nasdaq_trader as nasdaq
import pandas as pd
import pickle
import datetime as dt
import json
import os
import sys
import time
import requests
import bs4 as bs
import numpy as np

def get_nasdaq_tickers():
    tickers = nasdaq.get_nasdaq_symbols()

    with open('all_stock_tickers.pickle','wb') as f:
        pickle.dump(tickers.index.tolist(),f)

    return tickers.index.tolist()

def save_sp500_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class':'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)

    all_tickers = get_nasdaq_symbols()
    s_and_p = [x for x in tickers if x in all_tickers]

    with open("sp500tickers.pickle","wb") as f:
        pickle.dump(s_and_p,f)
    #print(tickers[:5])
    return all_tickers

def save_last_date(last_date=None):
    data = {}
    if last_date is not None and isinstance(last_date,(dt.date,dt.datetime)):
        TODAY = last_date
    else:
        TODAY = dt.date.today()

    data['last_check_date'] = {
        'year': TODAY.year,
        'month': TODAY.month,
        'day': TODAY.day
    }

    with open('date_data.txt','w') as f:
        json.dump(data,f)


def load_last_date(file, key='last_check_date'):
    with open(file,'r') as json_file:
        data = json.load(json_file)

    return dt.date(data[key]['year'],data[key]['month'], data[key]['day'])

def get_robinhood_data(start=dt.date(2010,1,1), end_date=dt.date.today()-dt.timedelta(1), ticker=None):

    tickers = get_nasdaq_tickers()

    if not os.path.exists('stock_dfs'):
        os.mkdir('stock_dfs')
    else:
        pass

    for ticker in tickers:
        if not os.path.exists(os.path.join('stock_dfs','{}.csv'.format(ticker))):
            print('{}'.format(ticker),end='\r\033[K')
            try:
                df = web.DataReader(ticker, 'robinhood', start, end_date)
                df.to_csv(os.path.join('stock_dfs','{}.csv'.format(ticker)))
            except KeyError:
                tickers.pop(tickers.index(ticker))
        elif start<(dt.date.today()-dt.timedelta(1)):
            print('{}'.format(ticker),end='\r')
            try:
                df = web.DataReader(ticker, 'robinhood', start, end_date)
                df.reset_index(inplace=True)
                df['begins_at'] = pd.to_datetime(df['begins_at'])
                #df.index = pd.to_datetime(df.index)
                old = pd.read_csv(os.path.join('stock_dfs','{}.csv'.format(ticker)))
                #old.set_index('begins_at', inplace=True)
                old['begins_at'] = pd.to_datetime(old['begins_at'])
                df = pd.concat([old, df])
                df = df.drop_duplicates(keep='first', subset='begins_at')
                df.to_csv(os.path.join('stock_dfs','{}.csv'.format(ticker)), index=False)
            except KeyError:
                tickers.pop(tickers.index(ticker))
                os.path.remove(os.path.join('stock_dfs','{}.csv'.format(ticker)))
        else:
            print('csv data is up to date')
            break
    save_last_date(dt.date.today()-dt.timedelta(1))

    return tickers

def compile_data(tickers=None):
    if tickers!=None:
        if isinstance(tickers,(list,tuple)):
            tickers = tickers
        elif isinstance(tickers,str):
            tickers = [tickers]
        else:
            raise TypeError('tickers expected str, list, or tuple type got {} instead.'.format(type(tickers)))
    elif os.path.exists('date_data.txt'):
        tickers = get_robinhood_data(start=load_last_date('date_data.txt'))
    else:
        tickers = get_robinhood_data()

    main_df = pd.DataFrame()

    for count, ticker in enumerate(tickers):
        if not os.path.exists(os.path.join('stock_dfs','{}.csv'.format(ticker))):
            continue
        df = pd.read_csv(os.path.join('stock_dfs','{}.csv'.format(ticker)))

        df.rename(columns = {'begins_at':'date', 'close_price':ticker, 'high_price':'high', 'low_price':'low', 'open_price': 'open'}, inplace=True)
        df.set_index('date', inplace=True)
        df.drop(['open', 'high', 'low', 'symbol','interpolated', 'volume', 'session'], 1, inplace=True)

        if main_df.empty:
            main_df = df
        else:
            main_df = main_df.join(df, how='outer')

        if count % 5 == 0:
            print('{} files processed.'.format(count),end='\r')
    print('DataFrame Compiled and pickled.')
    main_df.to_pickle('CompiledStocks.pickle')

if __name__=="__main__":
#    os.getcwd()
#    os.chdir('application')
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    if os.path.exists('date_data.txt'):
        tickers = get_robinhood_data(start=load_last_date('date_data.txt'))
    else:
        tickers = get_robinhood_data()
    compile_data(tickers)
