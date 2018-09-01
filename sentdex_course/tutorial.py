## Tutorial code based on part 1 of the course at pythonprogramming.net

import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
import pandas as pd
import pandas_datareader.data as web
from alpha_vantage.timeseries import TimeSeries


style.use('ggplot')

start = dt.datetime(2010,1,1)
end = dt.datetime.today()

ts = TimeSeries(key="GVR4ZPBY2FUX530Z",output_format='pandas')
df2, meta_data = ts.get_daily_adjusted('TSLA',outputsize='full')
df2.columns.tolist()
col_heads = {'6. volume':'volume', '8. split coefficient':'split_coef', '7. dividend amount':'divedend', '5. adjusted close':'adj_close', '1. open':'open', '2. high':'high', '4. close':'close',  '3. low':'low'}
df2.columns = df2.columns.map(col_heads)
df2.index = pd.to_datetime(df2.index)

# df = web.DataReader('AAL', 'robinhood',start,end)
# df.reset_index(inplace=True,drop=False,level=0)
# for col in [x for x in df.columns.tolist() if '_price' in x]:
#     df[col] = df[col].astype(float)
# df['volume'] = df['volume'].astype(int)

df_ohlc = df2['adj_close'].resample('10D').ohlc()
df_volume = df2['volume'].resample('10D').sum()

df_ohlc.reset_index(drop=False,inplace=True)
df_ohlc['date'] = df_ohlc['date'].map(mdates.date2num)

df_ohlc.head()
ax1 = plt.subplot2grid((6,1),(0,0),rowspan=5, colspan=1)
ax2 = plt.subplot2grid((6,1),(5,0),rowspan=1, colspan=1, sharex=ax1)
ax1.xaxis_date()

candlestick_ohlc(ax1,df_ohlc.values, width=2, colorup='green')
ax2.fill_between(df_volume.index.map(mdates.date2num),df_volume.values,0)
plt.show()
