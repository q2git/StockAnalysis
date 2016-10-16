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
    stat = df.groupby(['code','date']).max() #.ix['sh'] #shanghai index
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
    # rise and fall ratio
    rise1 = np.where(s.p_change>=0.1, 1.0, 0).sum()
    fall1 = np.where(s.p_change<=-0.1, 1.0, 0).sum()
    szb1 = ((rise1) / s.code.count()) * 100
    xdb1 = ((fall1) / s.code.count()) * 100
     
    rise2 = np.where(s.p_change>=3, 1.0, 0).sum()
    fall2 = np.where(s.p_change<=-3, 1.0, 0).sum() 
    szb2 = ((rise2) / s.code.count()) * 100   
    xdb2 = ((fall2) / s.code.count()) * 100 
     
    rise3 = np.where(s.p_change>=7, 1.0, 0).sum()
    fall3 = np.where(s.p_change<=-7, 1.0, 0).sum() 
    szb3 = ((rise3) / s.code.count()) * 100   
    xdb3 = ((fall3) / s.code.count()) * 100
    
    # rising trend
    crit1 = (s.close>=s.ma5) & (s.ma5>=s.ma10) & (s.ma10>=s.ma20)\
             & (s.ma20>=s.ma30) & (s.ma30>=s.ma60)
    crit2 = (s.close>=s.ma20) & (s.ma20>=s.ma30) & (s.ma30>=s.ma60)
    sstd1 = (np.where(crit1, 1.0, 0).sum() / s.code.count()) * 100
    sstd2 = (np.where(crit2, 1.0, 0).sum() / s.code.count()) * 100  

    # falling trend
    crit1 = (s.close<s.ma5) & (s.ma5<s.ma10) & (s.ma10<s.ma20)\
             & (s.ma20<s.ma30) & (s.ma30<s.ma60)
    crit2 = (s.close<s.ma20) & (s.ma20<s.ma30) & (s.ma30<s.ma60)
    xdtd1 = (np.where(crit1, 1.0, 0).sum() / s.code.count()) * 100
    xdtd2 = (np.where(crit2, 1.0, 0).sum() / s.code.count()) * 100 
    
    return pd.Series([sstd1, sstd2, xdtd1, xdtd2, szb1, szb2, szb3, 
                      xdb1, xdb2, xdb3, ], 
                     index=['sstd1', 'sstd2', 'xdtd1', 'xdtd2', 'szb1', 
                     'szb2', 'szb3', 'xdb1', 'xdb2', 'xdb3', ])


    
def add_MAs(dataframe, *args):
    ''' add moving average to dataframe '''
    df = dataframe.set_index('date').sort_index()
    
    for day in args:
        if pd.__version__ >= '0.18':
            ma = df.groupby('code')['close'].rolling(day).mean()\
                 .reset_index().rename(columns={'close':'ma{}'.format(day)})
        else:
            #pandas ver<0.18
            _ma = df.groupby('code').apply(lambda x: pd.rolling_mean(x.close, window=day))
            temp = []
            for code in _ma.index:
                t = _ma.xs(code).to_frame().reset_index()
                t['code'] = code
                temp.append(t)
            ma = pd.concat(temp).rename(columns={0:'ma{}'.format(day)})    

            
        dataframe = pd.merge(dataframe, ma, on=['code', 'date'])
    
    return dataframe    


    
def stat(years='2016', stk='000001', window=10):
    
    fn = os.path.join(STAT_FOLDER, 'stat_day_{}.xlsx'.format(years))  
    
    dfidxs = pd.DataFrame()
    dfstks = pd.DataFrame()
    for year in years.split('.'):
        dfidxs = dfidxs.append(db_to_df(year, table='indexs'))
        dfstks = dfstks.append(db_to_df(year))
        '''
        df1 = stat_indexs(db_to_df(year, table='indexs'))
        df2 = db_to_df(year)
        df2['ma30']=pd.rolling_mean(df2.close,window=30,)
        df2['ma60']=pd.rolling_mean(df2.close,window=60,)
        gb = df2.groupby('date')  
        df0 = gb.apply(fun1)    
        #for idx in set(df1.index.get_level_values(0)):
        #    df['id_{}'.format(idx.lower())] = df1.ix[idx].close
        df_idx =  df1.ix['sh',['close']].rename(columns={'close':'idx'})
        #df0['idx'] = df1.ix['sh'].close #sh index
        df_stk = df2[df2.code==stk].set_index('date')
        df_stk = df_stk[['close']].rename(columns={'close':stk})
        df0 = pd.concat([df0, df_stk, df_idx], axis=1, join_axes=[df_idx.index])
        
        #stk = df2.groupby('code').get_group('600596').set_index('date').close #
        dfs.append(df0) 
        '''
    dfidxs = dfidxs.groupby(['code','date']).max() 
    dfidx = dfidxs.ix['sh',['close']].rename(columns={'close':'idx'})
    
    dfstks = add_MAs(dfstks, 30, 60)
    dfstk = dfstks[dfstks['code']==stk].set_index('date')[['close']]\
            .rename(columns={'close':stk})
    
    dfgb = dfstks.groupby('date').apply(fun1)
    df = pd.concat([dfidx, dfstk, dfgb], axis=1, join_axes=[dfidx.index])

    
    #df = pd.concat(dfs)
    #window = 5
    for name in ['szb1', 'szb2', 'szb3', 'xdb1', 'xdb2', 'xdb3', ]:
        df[name] = df[name].rolling(window=window).mean()
  
    df.to_excel(fn)                
    return df#[['over_ma', 'rise0', 'rise3', stk, 'idx']]



def plot1(df, stk):
    fig, axes = plt.subplots(5, )#figsize=(6, 6))
    #plt.subplots_adjust(wspace=0.5, hspace=0.5)
    #fig, axes = plt.subplots(nrows=2, ncols=2) 
    #target = [axes[0][0],axes[0][1],axes[1][0],axes[1][1]]

    df1 = df.ix[:, [stk,'idx']]
    df2 = df.ix[:, ['sstd1', 'sstd2', 'xdtd1', 'xdtd2', 'idx']]
    df3 = df.ix[:, ['szb1', 'szb2', 'szb3', 'idx']]
    df4 = df.ix[:, ['xdb1', 'xdb2', 'xdb3', 'idx']]
    
    df1.plot(kind='line', secondary_y=stk, grid=True, subplots=False, 
             legend=True, rot='vertical',
             xticks=np.arange(0, len(df), 10))  

    df2.plot(subplots=True, ax=axes, legend=False,  grid=True, rot='vertical',
             sharex=True, figsize=(10, 8), xticks=np.arange(0, len(df2), 10),
             ) 
             
    [ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5)) for ax in axes]
             
    df3.plot(kind='line', grid=True, subplots=True, legend=False, rot='vertical',
                 xticks=np.arange(0, len(df), 10))
                 
    [ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5)) for ax in plt.gcf().axes]

    df4.plot(kind='line', grid=True, subplots=True, legend=False, rot='vertical',
                 xticks=np.arange(0, len(df), 10))  
                 
    [ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5)) for ax in plt.gcf().axes]
    '''         
        fig = plt.gcf() 
              
        for ax in axes:
            ax.legend(loc='upper left') #, bbox_to_anchor=(1.0, 0.5))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))#(ticker.MaxNLocator(6))            
            ax.xaxis.set_minor_locator(mdates.DayLocator(interval=5))
            #ax.xaxis.set_minor_locator(ticker.AutoMinorLocator(5))            
            ax.xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
            #ax.xaxis.set_major_formatter(mdates.DateFormatter('%d'))  
            ax.xaxis.grid(True, which="minor")
        #fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right') 
        plt.tick_params(axis='both', which='major', labelsize=10)
        plt.tick_params(axis='x', which='minor', labelsize=6)         
        fig.set_size_inches(16, 10)                
        fig.savefig(os.path.join(STAT_FOLDER, key), dpi=200) 
    '''
   
def main(): 
    years = raw_input('Years(eg, 2015.2016) or all: ')
    stk = raw_input('Stk code: ')
    w = input('window: ')
    
    df = stat(years, stk, w)
    
    plot1(df, stk)
   #plt.tight_layout()     
    plt.show() 
 
   
if __name__ == '__main__':
    main()

    #raw_input('END.') #press any key to exit
