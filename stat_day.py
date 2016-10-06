# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 09:07:52 2016

@author: q2git

# reading stock data from database and analysising it
"""

import matplotlib.pyplot as plt
import pandas as pd
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
    df['ZT'] = df.p_change>=9.9 
    df['DT'] = df.p_change<=-9.9  
    df['Z5'] = df.p_change>=5
    df['D5'] = df.p_change<=-5      
    #df['c5'] = df.close>=df.ma5
    #df['c10'] = df.close>=df.ma10
    #df['spjdy20'] = df.close>=df.ma20
    #df['v5'] = df.volume>=df.v_ma5
    #df['v10'] = df.volume>=df.v_ma10
    #df['cjldy20'] = df.volume>=df.v_ma20   
    stat = pd.pivot_table(df, 
                          values=['ZS','ZT','DT','Z5','D5',],
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

    ax1.set_ylabel('Z5', color='red')
    ax2.set_ylabel('D5', color='green') 
    ax3.set_ylabel('IDX_SH', color='blue')  
    
    plt.show()   
    
    
def main():
    year = raw_input('Year or all: ')
    fn = os.path.join(STAT_FOLDER, 'stat_{}.xlsx'.format(year))  
    
    s1 = stat_indexs(db_to_df(year, table='indexs'))
    s2 = stat_stocks(db_to_df(year))

    for idx in set(s1.index.get_level_values(0)):
        s2['IDX_{}'.format(idx.upper())] = s1.ix[idx].close
            
    with pd.ExcelWriter(fn) as writer:
        s2.to_excel(writer, sheet_name='Main')              
    
    return s2
    
    
if __name__ == '__main__':
    df = main()
    raw_input('END.') #press any key to exit