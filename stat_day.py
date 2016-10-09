# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 09:07:52 2016
@author: q2git
# reading stock data from database and analysising it
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3
import os


DATA_FOLDER = r'data\all'
STAT_FOLDER = r'data\stat'
if not os.path.exists(STAT_FOLDER):
    os.mkdir(STAT_FOLDER)

    
def db_to_df(year, ktype='D', table='stocks'):
    ''' reading data from db and return DataFrame object '''
    db = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, ktype))
    print 'reading [{}] data from [{}] ...'.format(table, db.upper()),       
    sql = 'select * from {}'.format(table)  #stocks or indexs 
    
    with sqlite3.connect(db) as con:
        #con.text_factory = str  
        df = pd.read_sql(sql, con)
    print 'Done.'
    return df


def stat_indexs(df):
    stat = df.groupby(['code','date']).mean() #.ix['sh'] #shanghai index
    return stat

 
def stat_stocks(df):
    df['ZS'] = 1
    #df['ZT'] = df.p_change>=9.9 
    #df['DT'] = df.p_change<=-9.9  
    df['SZ5'] = df.p_change>=5
    df['XD5'] = df.p_change<=-5 
    df['SZ'] = df.p_change>=0.01 
    df['XD'] = df.p_change<=-0.01     
    #df['c5'] = df.close>=df.ma5
    #df['c10'] = df.close>=df.ma10
    #df['spjdy20'] = df.close>=df.ma20
    #df['v5'] = df.volume>=df.v_ma5
    #df['v10'] = df.volume>=df.v_ma10
    #df['cjldy20'] = df.volume>=df.v_ma20   
    stat = pd.pivot_table(df, 
                          values=['ZS','SZ5','XD5','SZ','XD',],
                          index=['date'], aggfunc='sum',
                          )
    return stat

    
    
def plot(df):
    #fig = plt.figure() # Create matplotlib figure
    #ax1 = fig.add_subplot(111) # Create matplotlib axes
    fig, ax1 = plt.subplots()    
    ax2 = ax1.twinx() # Create another axes that shares the same x-axis as ax.
    ax3 = ax1.twinx()
    #ax3.set_frame_on(True)
    #ax3.patch.set_visible(False)
    fig.subplots_adjust(right=0.75) #shrink fig to 75%, left 25% for ax3
    rspine = ax3.spines['right']
    rspine.set_position(('axes', 1.25)) #extend ax3 to 125%
    
    df.Z5.plot(kind='line', color='red', ax=ax1, )
    df.D5.plot(kind='line', color='green', ax=ax2, )
    df.IDX_SH.plot(kind='line', color='blue', ax=ax3, ) 

    ax1.set_ylabel('SZ', color='red')
    ax2.set_ylabel('XD', color='green') 
    ax3.set_ylabel('IDX_SH', color='blue')  
    
    plt.show()   

    

def fun1(s):
    #avg_close = s.close.mean()  #non-weighted  
    avg_close = np.multiply(s.close,s.volume).sum()/s.volume.sum() #weighted
    #avg_price = sum(map(lambda x:x[0]*x[1], zip(s.close,s.volume)))/s.volume.sum()
    
    #sum_vol = s.volume.sum()
    
    rise = np.where(s.p_change>=0.1, 1.0, 0).sum()
    #fall = np.where(s.p_change<=-0.1, 1.0, 0).sum()
    #rf_ratio = rise/fall if rise>=fall else -fall/rise 
    rise0 = ((rise) / s.code.count()) * 100
     
    rise_3 = np.where(s.p_change>=3, 1.0, 0).sum()
    #fall_3 = np.where(s.p_change<=-3, 1.0, 0).sum()
    #rf_ratio_3 = rise_3/fall_3 if rise_3>=fall_3 else -fall_3/rise_3  
    rise3 = ((rise_3) / s.code.count()) * 100   
     
    #zt = np.where(s.p_change>=9.9, 1, 0).sum()
    #dt = np.where(s.p_change<=9.9, 1, 0).sum()
    crit = (s.close>=s.ma20) & (s.ma20>=s.ma30) & (s.ma30>=s.ma60)
    over_ma = (np.where(crit, 1.0, 0).sum() / s.code.count()) * 100
    
    return pd.Series([over_ma, avg_close, rise0, rise3, ], 
                     index=['over_ma', 'avg_close', 'rise0', 'rise3', ])
    
    
def main():
    years = raw_input('Years(eg, 2015.2016) or all: ')
    fn = os.path.join(STAT_FOLDER, 'stat_day_{}.xlsx'.format(years))     
    dfs = []
    for year in years.split('.'):
        df1 = stat_indexs(db_to_df(year, table='indexs'))
        df2 = db_to_df(year)
        df2['ma30']=pd.rolling_mean(df2.close,window=30,)
        df2['ma60']=pd.rolling_mean(df2.close,window=60,)
        gb = df2.groupby('date')  
        df0 = gb.apply(fun1)    
        #for idx in set(df1.index.get_level_values(0)):
        #    df['id_{}'.format(idx.lower())] = df1.ix[idx].close
        df0['idx'] = df1.ix['sh'].close #sh index
        stk = df2.groupby('code').get_group('600596').set_index('date').close #
        df0 = pd.concat([df0, stk], axis=1)
        dfs.append(df0) 
        
    df = pd.concat(dfs)
    w = 10 #input('window: ')
    df['rise3'] = pd.rolling_mean(df.rise3,window=w,)
    #df['avg_close'] = pd.rolling_mean(df.avg_close,window=w,)     
    df['rise0'] = pd.rolling_mean(df.rise0,window=w,) 
    #df['over_ma'] = pd.rolling_mean(df.over_ma,window=w,)     
    df.to_excel(fn)                
    return df
  
    
if __name__ == '__main__':
    df = main()
    df = df[['over_ma', 'rise0', 'rise3', 'avg_close', 'idx']]
    df.plot(kind='line', grid=True, subplots=True, legend=False,
                 xticks=np.arange(0, len(df), 10))

    [ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5)) for ax in plt.gcf().axes]
    #plt.tight_layout()     
    plt.show()
   
    raw_input('END.') #press any key to exit
