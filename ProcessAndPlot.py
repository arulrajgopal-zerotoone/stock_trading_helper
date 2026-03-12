from pandas import read_csv, to_datetime
from datetime import date, timedelta
from mplfinance import plot

import matplotlib.pyplot as plt

stock_name = "ITC"

df = read_csv(f'Data\{stock_name}.csv', skiprows=3)
df.columns = ['Date', 'AdjClose','Close', 'High', 'Low', 'Open', 'Volume']

df['Date'] = to_datetime(df['Date'])

today = date.today()
limited_days = today - timedelta(365)
limited_data_df = df[df['Date'] >= str(limited_days)]

set_index_df = limited_data_df.set_index('Date')


fig, axes = plot(set_index_df, type='candle', style='charles', volume=False, title=f'{stock_name} Stock Price', returnfig=True)
fig.set_size_inches(16, 9)
plt.show()