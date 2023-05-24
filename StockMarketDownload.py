import pandas as pd
from datetime import date
from jugaad_data.nse import stock_df

index_500 = pd.read_csv('ind_nifty500list.csv')

df = pd.DataFrame({'DATE':pd.Series(dtype = 'datetime64[ns]'),
                   'SERIES':pd.Series(dtype = 'object'),
                   'OPEN': pd.Series(dtype = 'float64'),
                   'HIGH': pd.Series(dtype = 'float64'),
                   'LOW': pd.Series(dtype = 'float64'),
                   'PREV. CLOSE': pd.Series(dtype = 'float64'),
                   'LTP': pd.Series(dtype = 'float64'),
                   'CLOSE': pd.Series(dtype = 'float64'),
                   'VWAP': pd.Series(dtype = 'float64'),
                   '52W H': pd.Series(dtype = 'float64'),
                   '52W L': pd.Series(dtype = 'float64'),
                   'VOLUME': pd.Series(dtype = 'int64'),
                   'VALUE': pd.Series(dtype = 'float64'),
                   'NO OF TRADES': pd.Series(dtype = 'int64'),
                   'SYMBOL': pd.Series('object')
                   })

df1 = df.copy()
for symbol in index_500['Symbol']:
    try:
        df_new = stock_df(symbol=symbol, from_date=date(2010,1,1), to_date=date(2020,1,1))
        df1 = pd.concat([df1, df_new], ignore_index=True)
        print(symbol,'done')
    except:
        print(symbol, 'omitted')
        continue
stockmarketprice = df1[['DATE','SYMBOL','CLOSE','PREV. CLOSE']]
stockmarkettrade = df1[['DATE','SYMBOL','VOLUME','NO OF TRADES']]
stockmarketprice.to_csv('StockMarketPrice.csv',index=False)
stockmarkettrade.to_csv('StockMarketTrade.csv',index=False)
print('Data Downloaded Successfully')