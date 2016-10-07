# -*- coding: utf-8 -*-
"""
Created on Thu Oct 06 21:15:10 2016

@author: q2git
"""
import pandas as pd

try:
    from pandas_datareader import data
except ImportError:
    from pandas.io import data

import datetime    


start = datetime.datetime(2013, 1, 1)
end = datetime.datetime(2016, 12, 27)
df1 = data.DataReader("600596.SS", 'yahoo', start, end)
df2 = data.DataReader("300331.SZ", 'yahoo', start, end)

print df1.head()
print df2.tail()