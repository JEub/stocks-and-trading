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
import matplotlib.pyplot as plt
from matplotlib import style
import matplotlib.dates as mdates
import json

import numpy as np

style.use('ggplot')

def visualize_correlation():
    df = pd.read_csv('sp500joindata.csv')
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date')

    df_corr = df.corr()

    data = df_corr.values
    fig, ax = plt.subplots()
    heatmap = ax.pcolor(data, cmap=plt.cm.RdYlGn)
    fig.colorbar(heatmap)
    ax.set_xticks(np.arange(data.shape[0])+0.5, minor=False)
    ax.set_yticks(np.arange(data.shape[1])+0.5, minor=False)
    ax.invert_yaxis()
    ax.xaxis.tick_top()

    column_labels = df_corr.columns.tolist()
    row_labels = df_corr.index.tolist()

    ax.set_xticklabels(column_labels)
    ax.set_yticklabels(row_labels)
    plt.xticks(rotation=90)
    heatmap.set_clim(-1,1)
    plt.tight_layout()
    plt.show()

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

def get_data_from_robinhood(reload_sp500=False, start=dt.datetime(2010,1,1), end=dt.datetime.today(), update=True, ticker=None):
    if ticker is not None:
        if isinstance(ticker,str):
            tickers = [ticker,]
        else:
            tickers = ticker
    else:
        if not os.path.exists('sp500tickers.pickle') and not reload_sp500:
            tickers = save_sp500_tickers()
        elif reload_sp500:
            tickers = save_sp500_tickers()
        else:
            with open("sp500tickers.pickle",'rb') as f:
                tickers = pickle.load(f)

    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    last_update = retrieve_last_date('date_data.txt')
    if last_update == end:
        update=False

    for ticker in tickers:
        sys.stdout.write('\r\033[K')
        sys.stdout.write('Processing {}.'.format(ticker))
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            try:
                df = web.DataReader(ticker, 'robinhood', start, end)
                df.to_csv('stock_dfs/{}.csv'.format(ticker))
            except KeyError:
                print("Ticker {} no longer in S&P 500.".format(ticker))
                reload_sp500 = True
        elif update:
            try:
                df = web.DataReader(ticker, 'robinhood', last_update, end)
                df.reset_index(inplace=True)
                df['begins_at'] = pd.to_datetime(df['begins_at'])
                #df.index = pd.to_datetime(df.index)
                old = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
                #old.set_index('begins_at', inplace=True)
                old['begins_at'] = pd.to_datetime(old['begins_at'])
                df = pd.concat([old, df])
                df = df.drop_duplicates(keep='first', subset='begins_at')
                df.to_csv('stock_dfs/{}.csv'.format(ticker), index=False)
            except KeyError:
                print('KeyError for {}'.format(ticker))
                reload_sp500 = True
        else:
            print('Already have {}'.format(ticker))

    if reload_sp500:
        print('Reloading S&P500 Tickers')
        tickers = save_sp500_tickers()
        for ticker in tickers:
            if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
                try:
                    df = web.DataReader(ticker, 'robinhood', start, end)
                    df.to_csv('stock_dfs/{}.csv'.format(ticker))
                except:
                    print('{} could not be gathered.'.format(ticker))

    save_last_date(last_date = dt.date.today()-dt.timedelta(1))

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

def retrieve_last_date(file, key='last_check_date'):
    with open(file,'r') as json_file:
        data = json.load(json_file)

    return dt.date(data[key]['year'],data[key]['month'], data[key]['day'])

def compile_close_data():
    with open('sp500tickers.pickle', "rb") as f:
        tickers = pickle.load(f)

    main_df = pd.DataFrame()

    for count, ticker in enumerate(tickers):
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))

        df.rename(columns = {'begins_at':'date', 'close_price':ticker, 'high_price':'high', 'low_price':'low', 'open_price': 'open'}, inplace=True)
        df.set_index('date', inplace=True)
        df.drop(['open', 'high', 'low', 'symbol','interpolated', 'volume', 'session'], 1, inplace=True)

        if main_df.empty:
            main_df = df
        else:
            main_df = main_df.join(df, how='outer')

        if count % 5 == 0:
            sys.stdout.write('\r')
            sys.stdout.write('{} files processed.'.format(count))

    print(main_df.tail())
    main_df.to_csv('sp500joindata.csv')

if __name__=='__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    get_data_from_robinhood()
    compile_close_data()
