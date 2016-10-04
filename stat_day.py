# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 09:07:52 2016

@author: q2git

# reading stock data from database and analysising it
"""

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
    df[u'总数'] = 1
    df[u'涨停数'] = df.p_change>=9.98 
    df[u'跌停数'] = df.p_change<=-9.98  
    df[u'涨幅大于5'] = df.p_change>=5
    df[u'跌幅大于5'] = df.p_change<=-5      
    #df['c5'] = df.close>=df.ma5
    #df['c10'] = df.close>=df.ma10
    #df['spjdy20'] = df.close>=df.ma20
    #df['v5'] = df.volume>=df.v_ma5
    #df['v10'] = df.volume>=df.v_ma10
    #df['cjldy20'] = df.volume>=df.v_ma20   
    stat = pd.pivot_table(df, 
                          values=[u'总数',u'涨停数',u'跌停数',u'涨幅大于5',u'跌幅大于5',],
                          index=['date'], aggfunc='sum',
                          )
    return stat


def main():
    year = raw_input('Year or all: ')
    fn = os.path.join(STAT_FOLDER, 'stat_{}.xlsx'.format(year))  
    
    s1 = stat_indexs(db_to_df(year, table='indexs'))
    s2 = stat_stocks(db_to_df(year))

    for idx in set(s1.index.get_level_values(0)):
        s2['idx_{}'.format(idx)] = s1.ix[idx].close
            
    with pd.ExcelWriter(fn) as writer:
        s2.to_excel(writer, sheet_name='Main')              

    
    
if __name__ == '__main__':
    main()
    raw_input('END.') #press any key to exit