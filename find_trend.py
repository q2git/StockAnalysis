# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 16:53:08 2016
@author: q2git
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np
import sqlite3
import os



DATA_FOLDER = r'data\all'
STAT_FOLDER = r'data\stat'
if not os.path.exists(STAT_FOLDER):
    os.mkdir(STAT_FOLDER)


def add_MAs(df, *args):
    ''' add moving average to dataframe '''
    
    df0 = df.set_index('date').sort_index()
    
    for day in args:
        ma = df0.groupby('code')['close'].rolling(day).mean()\
             .reset_index().rename(columns={'close':'ma{}'.format(day)})
        
        df = pd.merge(df, ma, on=['code', 'date'])
    
    return df 

    
def db2df(years='2016', ktype='D', table='stocks'):
    """ reading data from db and return DataFrame object """
    
    df = pd.DataFrame()
    
    for year in years.split('.'):
 
        db = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, ktype))
    
        with sqlite3.connect(db) as con:
            sql = 'select * from {}'.format(table)  #stocks or indexs 
            
            print 'Reading table [{}] from [{}] ...'.format(table, db), 
            df = df.append(pd.read_sql(sql, con), ignore_index=True)
            print 'Done.'
    
    return df      


def fun1(s):
    """ apply function """
    kwargs = {}
    cnt = s.code.count()
    # rise and fall ratio
    for i in [0.1, 3, 7]:
        k1 = 'pc_a{:.0f}'.format(i)
        k2 = 'pc_b{:.0f}'.format(i)
        #c1 = np.where(s['p_change']>=i, 1.0, 0).sum() #rise
        #c2 = np.where(s['p_change']<=-i, 1.0, 0).sum() #fall
        kwargs[k1] = (np.where(s['p_change']>=i, 1.0, 0).sum())# / cnt) * 100
        kwargs[k2] = (np.where(s['p_change']<=-i, 1.0, 0).sum())# / cnt) * 100     

    # ma trends
    mas = [5, 10, 20, 30, 60]
    f = lambda x: 'ma{}'.format(x)

    for ma in mas:
        k1 = 'ma_a{:02d}'.format(ma) #above
        k2 = 'ma_b{:02d}'.format(ma) #below        
        kwargs[k1] = (np.where(s['close']>=s[f(ma)], 1.0, 0).sum() / cnt) * 100        
        kwargs[k2] = (np.where(s['close']<s[f(ma)], 1.0, 0).sum() / cnt) * 100    
    
    for i in [5, 20]:
        idx = mas.index(i)
        _mas = zip(mas[idx:], mas[idx+1:])
        k1 = 'ma_u{:02d}'.format(i) #up trend
        k2 = 'ma_d{:02d}'.format(i) #down trend 
        c1 = reduce(lambda m,n: m&n, map(lambda (x,y):s[f(x)]>=s[f(y)], _mas))
        c2 = reduce(lambda m,n: m&n, map(lambda (x,y):s[f(x)]<s[f(y)], _mas)) 
        kwargs[k1] = (np.where(c1, 1.0, 0).sum() / cnt) * 100
        kwargs[k2] = (np.where(c2, 1.0, 0).sum() / cnt) * 100     

    ser = pd.Series(data=kwargs.values(), index=kwargs.keys()).sort_index()
    
    return ser                    
                     
                     
def stat(years='2016', window=None):
    """ statistic analysis """
    
    dfidxs = db2df(years, table='indexs')
    dfidxs = dfidxs.groupby(['code','date'])['close'].max().unstack(0)
    dfidx = dfidxs['sh'].rename('idx')
    
    dfstks = db2df(years)
    dfstks = add_MAs(dfstks, 30, 60)

    df = dfstks.groupby('date').apply(fun1)
    
    if window:
        for colname in df.columns:
            if not colname.startswith('ma_'):
                df[colname] = df[colname].rolling(window=window).mean()

    df = pd.concat([dfidx, df], axis=1, join_axes=[dfidx.index]) #.sort_index(1)
     
    return df

    
def plot1(df):
    
    #df.index = pd.to_datetime(df.index, format="%Y-%m-%d") #.to_period(freq='D')
    df.index = pd.to_datetime(df.index).to_period(freq='D')
    
    for key in ['pc_a','pc_b','ma_a','ma_b','ma_u','ma_d']:
        ks = []

        for n in df.columns:
            if n.startswith(key):
                ks.append(n)
                
        ks.append('idx')

        axes = df[ks].plot(kind='line', grid=True, subplots=True, legend=False, 
                            #x_compat=True,
                            )

        fig = plt.gcf() 
              
        for ax in axes:
            ax.legend(loc='upper left') #, bbox_to_anchor=(1.0, 0.5))
            ax.xaxis.set_major_locator(ticker.MaxNLocator(5))            
            #ax.xaxis.set_minor_locator(mdates.DayLocator(interval=5))
            ax.xaxis.set_minor_locator(ticker.AutoMinorLocator(3))            
            ax.xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
            
        #fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right') 
            
        fig.set_size_inches(16, 10)                
        fig.savefig(os.path.join(STAT_FOLDER, key), dpi=200)               
 
    
def main():
    years = raw_input('Years(eg, 2015.2016) or all: ')
    w = raw_input('window: ')
    if w: w=int(w)
    
    df = stat(years, w)
    
    plot1(df)
 
    #plt.show() 
 

    
    
if __name__ == '__main__':
    main()
    
