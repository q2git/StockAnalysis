# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 09:07:52 2016

@author: q2git

# reading stock data from database and analysising it
"""

import pandas as pd
import sqlite3


def db_to_df(year, ktype='D', table='stocks'):
    ''' reading data from db and return DataFrame object '''
    db = '{}_{}.db'.format(year, ktype)
    print 'reading data from [{}] ...'.format(db.upper()),       
    sql = 'select * from {}'.format(table)  #stocks or indexs 
    con = sqlite3.connect(db)
    #con.text_factory = str  
    df = pd.read_sql(sql, con)
    print 'Done.'
    return df

    
def data_stat():
    df = db_to_df(2016)
    df['total'] = 1
    df['zts'] = df.p_change>=9.98 
    df['dts'] = df.p_change<=-9.98  
    df['zfdy5'] = df.p_change>=5
    df['dfdy5'] = df.p_change<=-5      
    #df['c5'] = df.close>=df.ma5
    #df['c10'] = df.close>=df.ma10
    df['spjdy20'] = df.close>=df.ma20
    #df['v5'] = df.volume>=df.v_ma5
    #df['v10'] = df.volume>=df.v_ma10
    df['cjldy20'] = df.volume>=df.v_ma20   
    stat = pd.pivot_table(df, 
                          values=['total','zts','dts','zfdy5','dfdy5',
                                  'spjdy20','cjldy20',],
                          index=['date'], aggfunc='sum',
                          )
    dfi = db_to_df(2016, table='indexs') #indexs
    idx=dfi.groupby(['code','date']).mean().ix['sh'] #shanghai index
    stat['idx_sh'] = idx.close
    return stat

if __name__ == '__main__':
    #df = db_to_df(2016)
    stat = data_stat()
    print stat
