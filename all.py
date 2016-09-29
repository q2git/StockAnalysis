# -*- coding: utf-8 -*-
"""
Created on Thu Sep 29 18:51:16 2016

@author: PYTHON
"""

import pandas as pd
import tushare as ts
import sqlite3

con = sqlite3.connect('stk.db')
con.text_factory = str

def basic():
    df = ts.get_stock_basics()
    df.to_sql(u'basic', con, if_exists='replace')
    
def hists(year):
    start = '{}-01-01'.format(year)
    end = '{}-12-31'.format(year)
    print 'From {} to {}'.format(start, end)
    data = pd.read_sql('select code from basic', con).code.values
    size = data.size
    codes = data.tolist()
    table = 'hists_{}'.format(year)
    for i in range(0, size, 10):
        symbols = codes[i:i+10]
        print 'processing codes: {}'.format(symbols)
        df = ts.get_hists(symbols, start=start, end=end)
        df.to_sql(table, con, if_exists='append')
        size -= 10
        print 'Done. {} codes left'.format(size)
    
if __name__ == '__main__':
    hists(2016)