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
import commons as cm

    
def db2df(years='2016', ktype='D', table='stocks'):
    """ reading data from db and return DataFrame object """
    
    df = pd.DataFrame()
    
    for year in years.split('.'):
 
        db = os.path.join(cm.DATA_DIR, '{}_{}.db'.format(year, ktype))
    
        with sqlite3.connect(db) as con:
            sql = 'select * from {}'.format(table)  #stocks or indexs 
            
            print 'Reading table [{}] from [{}] ...'.format(table, db), 
            df = df.append(pd.read_sql(sql, con), ignore_index=True)
            print 'Done.'
    
    return df      


def add_Cols(df, **kwargs):
    ''' add moving average to dataframe '''
    
    print 'Adding columns to dataframe...',
    mas = kwargs.setdefault('ma', [30, 60])
    rds = kwargs.setdefault('rds', [60, 90])
    
    df0 = df.set_index('date').sort_index()
    
    for day in mas:
        ma = df0.groupby('code')['close'].rolling(day).mean()\
             .reset_index().rename(columns={'close':'ma{}'.format(day)})
        
        df = pd.merge(df, ma, on=['code', 'date'])
        
    for day in rds:
        c_max = df0.groupby('code')['close'].rolling(day).max()\
                .reset_index().rename(columns={'close':'roll_max{}'.format(day)})
                
        c_min = df0.groupby('code')['close'].rolling(day).min()\
                .reset_index().rename(columns={'close':'roll_min{}'.format(day)})
    
        df = pd.merge(df, c_max, on=['code', 'date'])
        df = pd.merge(df, c_min, on=['code', 'date'])

    print 'Done.'
    
    return df 
    

def fun1(s):
    """ apply function """
    
    kwargs = {}
    pct_coff = 100.0/s.code.count() #to percentage
    # p change
    for i in [1, 7]:
        k1 = 'pc_pos{:.0f}'.format(i)
        k2 = 'pc_neg{:.0f}'.format(i)
        kwargs[k1] = np.where(s['p_change']>=i, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(s['p_change']<=-i, 1.0, 0).sum() * pct_coff   

    # ma trends
    mas_c = [5, 10, 20, 30, 60]
    f = lambda x: 'ma{}'.format(x)

    for ma in mas_c[::2]:
        k1 = 'over_ma{}'.format(ma) #above      
        kwargs[k1] = np.where(s['close']>=s[f(ma)], 1.0, 0).sum() * pct_coff          
    
    for i in [5, 20]:
        idx = mas_c.index(i)
        _mas = zip(mas_c[idx:], mas_c[idx+1:])
        k1 = 'trend_rise{}'.format(i) #up trend (close>=ma5>=10>=20...)
        k2 = 'trend_fall{}'.format(i) #down trend (close<ma5<10<20...)
        c1 = reduce(lambda m,n: m&n, map(lambda (x,y):s[f(x)]>=s[f(y)], _mas))
        c2 = reduce(lambda m,n: m&n, map(lambda (x,y):s[f(x)]<s[f(y)], _mas)) 
        kwargs[k1] = np.where(c1, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(c2, 1.0, 0).sum() * pct_coff     

    # close, amplitude, volumn
    kwargs['avg_close'] =  np.multiply(s['close'], s['volume']).sum() / s['volume'].sum()
    kwargs['avg_swing'] = (((s['high']-s['low']) / s['low']).mean()) * 100
    kwargs['avg_volume'] = s['volume'].mean()          
 
    # expanding_max
    for i in [60, 90]:
        k1 = 'roll_max{}'.format(i) #above
        k2 = 'roll_min{}'.format(i) #below          
        kwargs[k1] = np.where(s['close']>=s[k1], 1.0, 0).sum() 
        kwargs[k2] = np.where(s['close']<=s[k2], 1.0, 0).sum() 
    
    ser = pd.Series(data=kwargs.values(), index=kwargs.keys()).sort_index()
    
    return ser                    
                     
                     
def stat(years='2016', w=None):
    """ statistic analysis """
    
    dfidxs = db2df(years, table='indexs')
    dfidxs = dfidxs.groupby(['code','date'])['close'].max().unstack(0)
    dfidxs.rename(columns=lambda x:'idx_{}'.format(x), inplace=True)
    #dfidx = dfidxs[idx].rename('zs_{}'.format(idx))
    
    dfstks = db2df(years)
    dfstks = add_Cols(dfstks)

    print 'Performing statistics...',
    
    df = dfstks.groupby('date').apply(fun1)
   
    if w:
        for colname in df.columns:
            if colname.startswith(('pc_','over_ma', 'roll')):
                df[colname] = df[colname].rolling(window=w).mean()
                df.rename(columns={colname:'{}_w{}'.format(colname, w)},inplace=True)

    df = pd.concat([dfidxs, df], axis=1, join_axes=[dfidxs.index]) #.sort_index(1)
    #df.to_excel('stat.xlsx')     
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d") #.to_period(freq='D')
    #df.index = pd.to_datetime(df.index).to_period(freq='D')    
    print 'Done.'
    
    return df

    
def plot1(df, ref='sh'):
    
    print 'Preparing for figures...',

    colnames = df.columns
    #ref = next((x for x in colnames if x.startswith('zs_')))
    ref = 'idx_{}'.format(ref) 
    
    for key in ['pc','over_ma','trend','avg','roll','idx']:
        
        ks = filter(lambda x: x.startswith(key), colnames)
        if key != 'idx': 
            ks.append(ref)

        axes = df[ks].plot(kind='line', grid=True, subplots=True, legend=False, 
                           rot=0, marker='.',)

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
        fn = '{}_{}'.format(cm.TODAY, key)               
        fig.savefig(os.path.join(cm.STAT_DIR, fn), dpi=200) 

    print 'Done.'               

        
def main():
    years = raw_input('Years(=2016, all): ')
    if not years: years='2016'
    
    idx = raw_input('Index(=sh,sz,hs300,sz50,zxb,cyb): ')
    if not idx: idx='sh'
    
    w = raw_input('Window size(=None): ')
    if w: w=int(w)
    
    df = stat(years, w)
    from plottings import plot_pair, save_fig
    figs = plot_pair(df)
    save_fig(**figs)
    #plot1(df, idx)
 
    if raw_input('Show plot?: '):
        plt.show() 
    
    
if __name__ == '__main__':
    main()
