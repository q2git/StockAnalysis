# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 16:53:08 2016
@author: q2git
"""

import pandas as pd
import numpy as np
import sqlite3
import os


DATA_DIR =  'data'
STAT_DIR =  'stat'
for folder in [DATA_DIR, STAT_DIR]:
    if not os.path.exists(folder):
        os.mkdir(folder) 
        
        
def db2df(years='2016', ktype='D', table='stocks', que=None):
    """ reading data from db and return a generator """
    
    for year in years.split('.'):
 
        db = os.path.join(DATA_DIR, '{}_{}.db'.format(year, ktype))
        
        msg = 'Reading table [{}] from [{}] ...'.format(table, db)
        if que is not None:
            que.put((0,msg))
        else:
            print msg, 
            
        with sqlite3.connect(db) as con:
            sql = 'select * from {}'.format(table)  #stocks or indexs 
            df = pd.read_sql(sql, con)
            df.drop_duplicates(inplace=True)
        # convert to float32 for saving memory cost    
        cols = df.columns.tolist()
        cols.remove('date')
        cols.remove('code')
        df[cols] = df[cols].astype('float32')
        
        if que is not None:
            que.put((0,'Done.\r\n'))
        else:
            print 'Done.' 
            
        yield df      


def add_cols(df, ma_days=[30,60], que=None):
    """ add columns moving average and rolling-max/min to dataframe 
    usage: df = add_MAs_RMs(df, mas=[30,60],) """
    
    msg = 'Adding columns ma{} to df...'.format(ma_days)
    if que is not None:
        que.put((0,msg))
    else:
        print msg,
        
    try:            
        df0 = df.set_index('date').sort_index()
        
        for day in ma_days:
            ma = df0.groupby('code')['close'].rolling(int(day)).mean()\
                 .reset_index().rename(columns={'close':'ma{}'.format(day)})
            
            df = pd.merge(df, ma, on=['code', 'date'])
        '''    
        for day in rmxx_days:
            c_max = df0.groupby('code')['close'].rolling(int(day)).max()\
                    .reset_index().rename(columns={'close':'rmax{}'.format(day)})
                    
            #c_min = df0.groupby('code')['close'].rolling(int(day)).min()\
            #        .reset_index().rename(columns={'close':'rmin{}'.format(day)})
        
            df = pd.merge(df, c_max, on=['code', 'date'])
            #df = pd.merge(df, c_min, on=['code', 'date'])
        '''     
        msg = 'Done.'    
    except Exception as e:
        msg = '{}'.format(e)
        
    if que is not None:
        que.put((0, msg+'\n'))
    else:
        print msg
    
    return df 
    

def stat_daily(s):
    """ apply function for daily """
    
    kwargs = {}
    pct_coff = 100.0/s.code.count() #to percentage
    
    # p change
    if 'p_change' in s.columns:
        p_changes=[1,5,9]
    else: # QFQ data has no p_change column
        p_changes=[]
    for i in p_changes:
        k1 = 'p_change: >+{:.0f}%'.format(i)
        k2 = 'p_change: <-{:.0f}%'.format(i)
        kwargs[k1] = np.where(s['p_change']>=i, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(s['p_change']<=-i, 1.0, 0).sum() * pct_coff 

    # get column name maxx
    mas = s.columns.str.extract('(^ma\d+)', expand=False).dropna().tolist()
    mas.sort(key=lambda x: int(x[2:])) 
    # close > ma, bias
    for ma in mas:
        bias = ((s['close']-s[ma]) / s[ma]) * 100
        k1 = 'close: >{}'.format(ma)
        k2 = 'bias: {}'.format(ma)         
        kwargs[k1] = np.where(s['close']>=s[ma], 1.0, 0).sum() * pct_coff 
        kwargs[k2] = bias.mean() #np.where(bias>=10, 1.0, 0).sum() * pct_coff    
    
    #trends[0] means close>ma5>ma10>ma..
    trends=[0,1,2]
    for i in trends:
        _mas = mas[i:]
        _mas.insert(0,'close')
        cmp_pairs = zip(_mas[i:], _mas[i+1:])
        k1 = 'trend: ' + '>'.join(_mas[:2]) #up trend (close>=ma5>=10>=20...)
        k2 = 'trend: ' +'<'.join(_mas[:2]) #down trend (close<ma5<10<20...)
        c1 = reduce(lambda m,n: m&n, map(lambda (x,y):s[x]>=s[y], cmp_pairs))
        c2 = reduce(lambda m,n: m&n, map(lambda (x,y):s[x]<s[y], cmp_pairs)) 
        kwargs[k1] = np.where(c1, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(c2, 1.0, 0).sum() * pct_coff     
    '''
    # rolling_max/min
    rmaxs = s.columns.str.extract('(^rmax\d+)', expand=False).dropna().tolist()
    #rmins = s.columns.str.extract('(^rmin\d+)', expand=False).dropna().tolist()
    for rmax in rmaxs: #, rmin in zip(rmaxs, rmins):
        k1 = 'close=: {}'.format(rmax) #close = highest
        #k2 = 'close=: {}'.format(rmin) #close = lowest          
        kwargs[k1] = np.where(s['close']>=(s[rmax]*0.95), 1.0, 0).sum() * pct_coff
        #kwargs[k2] = np.where(s['close']<=(s[rmin]*1.05), 1.0, 0).sum() * pct_coff
    
    # volumn_maxx
    v_mas = s.columns.str.extract('(^v_ma\d+)', expand=False).dropna().tolist()
    v_mas.sort(key=lambda x: int(x[4:])) 
    v_mas.insert(0,'volume')
    cmp_v_mas = zip(v_mas, v_mas[1:])
    c = reduce(lambda m,n: m&n, map(lambda (x,y):s[x]>=s[y], cmp_v_mas))
    kwargs['volumn: trend'] = np.where(c, 1.0, 0).sum() * pct_coff   
    #for v_ma in v_mas:
    #    k1 = 'volume: >{}'.format(v_ma) #above      
    #    kwargs[k1] = np.where(s['volume']>=s[v_ma], 1.0, 0).sum() * pct_coff  
    '''
    # close, swing, volumn
    kwargs['avg: close'] =  s['close'].mean()
    kwargs['avg: swing'] = ((s['high']-s['low']) / s['low']).mean() * 100
    kwargs['avg: volume'] = s['volume'].mean()  
    #kwargs['avg: turnover'] = s['turnover'].mean() #(s['turnover'] * s['volume']).sum() / s['volume'].sum()       
    #kwargs['swing: >7%'] = np.where(((s['high']-s['low'])/s['low'])>0.07, 1.0, 0).sum() * pct_coff
  
    ser = pd.Series(data=kwargs.values(), index=kwargs.keys()).sort_index()
    
    return ser                    


def df_idxs_codes(years='2016', ktype='D', add_mas=[30], que=None):
    """ get dfs of indexs and codes """
    # indexs
    dfi = pd.concat(db2df(years, ktype, table='indexs', que=que), 
                    ignore_index=True)
    dfi = dfi.groupby(['code','date'])['close'].max().unstack(0)
    dfi.rename(columns=lambda x:'idx: {}'.format(x), inplace=True)   
    # codes    
    dfc = pd.concat(db2df(years, ktype, que=que), ignore_index=True) 
    dfc = add_cols(dfc, add_mas,  que)

    if que is not None:
        que.put((2,(dfi,dfc))) 
        
    return (dfi, dfc)
                         
                     
def stat(dfi, dfc, k=None, f=None, w=None, que=None):
    """ statistic analysis """
    
    if que is not None:     
        que.put((0,'Performing statistics...'))
    else:
        print 'Performing statistics...',
    
    try:
        dfc = dfc.groupby('date').apply(stat_daily) # statistical analysis
        dfc = pd.concat([dfi, dfc], axis=1, join_axes=[dfi.index]) #.sort_index(1)
        
        if k and f and w:
            for n in dfc.columns:
                if n.startswith((k,)):
                    str_f = "dfc['{:s}'].rolling(window={:d}).{:s}()".format(n,w,f)
                    dfc[n] = eval(str_f) #dfc[n].rolling(window=w).mean()
                    dfc.rename(columns={n:'{}({}{})'.format(n,f,w)},inplace=True)
        msg = 'Done.'
    except Exception as e:
        msg = e

    
    if que is not None:
        que.put((0,'{}\n'.format(msg)))
        que.put((1,dfc))
    else:
        print  msg
        return dfc       
    
    
if __name__ == '__main__':
    years = raw_input('Years(=2016, 2015.2016): ')
    if not years: years='2016'
    
    w = raw_input('Window size(=None): ')
    if w: w=int(w)
    
    dfi, dfc = df_idxs_codes(years)
    df = stat(dfi, dfc, w)
    df.to_excel(os.path.join(STAT_DIR, 'stat_{}_daily.xlsx'.format(years)))     
