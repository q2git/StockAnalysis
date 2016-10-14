# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 16:53:08 2016
@author: q2git
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import sqlite3
import os
import datetime


TODAY = datetime.date.today()
DATA_FOLDER = r'data\all'
STAT_FOLDER = r'data\stat'
if not os.path.exists(STAT_FOLDER):
    os.mkdir(STAT_FOLDER)

    
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


def add_MAs(df, *args):
    ''' add moving average to dataframe '''
    
    print 'Adding columns to dataframe...',
    
    df0 = df.set_index('date').sort_index()
    
    for day in args:
        ma = df0.groupby('code')['close'].rolling(day).mean()\
             .reset_index().rename(columns={'close':'ma{}'.format(day)})
        
        df = pd.merge(df, ma, on=['code', 'date'])
  
    c_max = df0.groupby('code')['close'].expanding().max()\
            .reset_index().rename(columns={'close':'c_max'})
            
    c_min = df0.groupby('code')['close'].expanding(center=True).min()\
            .reset_index().rename(columns={'close':'c_min'})
    
    df = pd.merge(df, c_max, on=['code', 'date'])
    df = pd.merge(df, c_min, on=['code', 'date'])

    print 'Done.'
    
    return df 
    

def fun1(s):
    """ apply function """
    
    kwargs = {}
    pct_coff = 100/s.code.count() #to percentage
    # p change
    for i in [0.1, 7]:
        k1 = 'pc_A{:.0f}'.format(i)
        k2 = 'pc_B{:.0f}'.format(i)
        kwargs[k1] = np.where(s['p_change']>=i, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(s['p_change']<=-i, 1.0, 0).sum() * pct_coff   

    # ma trends
    mas_c = [5, 10, 20, 30, 60]
    f = lambda x: 'ma{}'.format(x)

    for ma in mas_c[::2]:
        k1 = 'ma_A{:02d}'.format(ma) #above
        k2 = 'ma_B{:02d}'.format(ma) #below        
        kwargs[k1] = np.where(s['close']>=s[f(ma)], 1.0, 0).sum() * pct_coff        
        kwargs[k2] = np.where(s['close']<s[f(ma)], 1.0, 0).sum() * pct_coff   
    
    for i in [5, 20]:
        idx = mas_c.index(i)
        _mas = zip(mas_c[idx:], mas_c[idx+1:])
        k1 = 'ma_U{:02d}'.format(i) #up trend
        k2 = 'ma_D{:02d}'.format(i) #down trend 
        c1 = reduce(lambda m,n: m&n, map(lambda (x,y):s[f(x)]>=s[f(y)], _mas))
        c2 = reduce(lambda m,n: m&n, map(lambda (x,y):s[f(x)]<s[f(y)], _mas)) 
        kwargs[k1] = np.where(c1, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(c2, 1.0, 0).sum() * pct_coff     

    # close, amplitude, volumn
    kwargs['avg_c'] =  np.multiply(s['close'], s['volume']).sum() / s['volume'].sum()
    kwargs['avg_a'] = (((s['high']-s['low']) / s['low']).mean()) * 100
    kwargs['avg_v'] = s['volume'].mean()          
 
    #above expanding_max, below expanding_min
    kwargs['c_max'] = np.where(s['close']>=s['c_max'], 1.0, 0).sum() #/ cnt) * 100
    kwargs['c_min'] = np.where(s['close']<=s['c_min'], 1.0, 0).sum() #/ cnt) * 100
    
    ser = pd.Series(data=kwargs.values(), index=kwargs.keys()).sort_index()
    
    return ser                    
                     
                     
def stat(years='2016', w=None):
    """ statistic analysis """
    
    dfidxs = db2df(years, table='indexs')
    dfidxs = dfidxs.groupby(['code','date'])['close'].max().unstack(0)
    dfidxs.rename(columns=lambda x:'zs_{}'.format(x), inplace=True)
    #dfidx = dfidxs[idx].rename('zs_{}'.format(idx))
    
    dfstks = db2df(years)
    dfstks = add_MAs(dfstks, 30, 60)

    print 'Performing statistics...',
    
    df = dfstks.groupby('date').apply(fun1)
   
    if w:
        for colname in df.columns:
            if not colname.startswith('ma_'):
                df[colname] = df[colname].rolling(window=w).mean()
                df.rename(columns={colname:'{}_{}'.format(colname, w)},inplace=True)

    df = pd.concat([dfidxs, df], axis=1, join_axes=[dfidxs.index]) #.sort_index(1)

    print 'Done.'
    
    return df

    
def plot1(df, ref='sh'):
    
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d") #.to_period(freq='D')
    #df.index = pd.to_datetime(df.index).to_period(freq='D') 

    colnames = df.columns
    #ref = next((x for x in colnames if x.startswith('zs_')))
    ref = 'zs_{}'.format(ref) 
    
    for key in ['pc_A','pc_B','ma_A','ma_B','ma_U','ma_D','avg_','c_','zs']:
        
        ks = filter(lambda x: x.startswith(key), colnames)
        if key != 'zs': 
            ks.append(ref)

        axes = df[ks].plot(kind='line', grid=True, subplots=True, legend=False, 
                           rot=0, )

        fig = plt.gcf() 
              
        for ax in axes:
            ax.legend(loc='upper left', prop={'size':8},) #, bbox_to_anchor=(1.0, 0.5))
            ax.xaxis.set_minor_locator(mdates.WeekdayLocator(interval=1))           
            ax.xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
            #ax.xaxis.grid(True, which="minor") 
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))            
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))  
            ax.tick_params(axis='both', which='both', labelsize=8)

        plt.tick_params(axis='x', which='minor', labelsize=6, )
        plt.tick_params(axis='x', which='major', pad=20, )        
        plt.setp(axes[-1].xaxis.get_minorticklabels(), rotation=90) 

        fig.set_size_inches(16, 9) 
        fn = '{}_{}'.format(key, TODAY)               
        fig.savefig(os.path.join(STAT_FOLDER, fn), dpi=200) 
               

        
def main():
    years = raw_input('Years(=2016, all): ')
    if not years: years='2016'
    
    idx = raw_input('Index(=sh,sz,hs300,sz50,zxb,cyb): ')
    if not idx: idx='sh'
    
    w = raw_input('Window size(=None): ')
    if w: w=int(w)
    
    df = stat(years, w)
    
    plot1(df, idx)
 
    if raw_input('Show plot?: '):
        plt.show() 
    
    
if __name__ == '__main__':
    main()
