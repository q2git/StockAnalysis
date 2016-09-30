# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 09:07:52 2016

@author: q2git
"""

import pandas as pd
import sqlite3


def read_hists_by_year(year):
    db = '{}.db'.format(year)
    sql = 'select * from hists_{}'.format(year)    
    con = sqlite3.connect(db)
    #con.text_factory = str  
    print 'reading data from [{}] ...'.format(db.upper()),
    df = pd.read_sql(sql, con)
    print 'Done.'
    return df
    
def data_stat(df):
    df['total'] = 1
    df['zt'] = df.p_change>=9.98 
    df['dt'] = df.p_change<=-9.98  
    df['zf5'] = df.p_change>=5
    df['df5'] = df.p_change<=-5      
    df['c5'] = df.close>=df.ma5
    df['c10'] = df.close>=df.ma10
    df['c20'] = df.close>=df.ma20
    df['v5'] = df.volume>=df.v_ma5
    df['v10'] = df.volume>=df.v_ma10
    df['v20'] = df.volume>=df.v_ma20   
    stat = pd.pivot_table(df, 
                          values=['total','zt','dt','zf5','df5',
                                  'c5','c10','c20','v5','v10','v20',],
                          index=['date'], aggfunc='sum',
                          )

    return stat

if __name__ == '__main__':
    df = read_hists_by_year(2016)
    stat = data_stat(df)
    print stat
