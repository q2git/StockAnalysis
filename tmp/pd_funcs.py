# -*- coding: utf-8 -*-
"""
Created on Sat Oct 08 11:29:07 2016

@author: q2git
"""

import pandas as pd
import numpy as np
import functools

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

df = pd.DataFrame

'''
df[col_name]
df[[col_name1,col_name2]]
df.ix[row, col]
df.ix[:2, :10]
df.ix[row, [col_name1,col_name2]]
df.ix[idx_name, col_name]
df.ix[idx_name1, idx_name2].col_name
'''
############ Cookbook ############

df.ix[df.p_change>=9.9, 'ZT'] = 1 #if-then
df['ZT'] = np.where(df.p_change>=9.9, 1, 0) #if-then-else
dfzt = df[df.p_change>=9.9] #splitting
dfzt = df.loc[(df.p_change>=9.9) & (df.open>9), 'close'] #and (return Series)
dfzt = df.loc[(df.p_change>=9.9) | (df.close>10), 'ZT'] = 1 #or (return modified DataFrame)
df.ix[(df.close-9).abs().argsort()] #Select rows with data closest to certain value using argsort
#Dynamically reduce a list of criteria using a binary operators
crit1 = df.close>10
crit2 = df.close>df.ma20
df[crit1&crit2] #hard code
df[functools.reduce(lambda x,y: x&y,[crit1,crit2])] #reduce function

#selection
df[~(df.close<100) & (df.p_change>9.9)] # ~ is inverse operator
df.loc['2016-09-01':'2016-09-20'] #Label-oriented, same as df.ix['2016-09-01':'2016-09-20']
df.ix[0:3] #same as df.iloc[0:3] Position-oriented

#df[new_cols] = df[source_cols].applymap(categories.get)
df.loc[df.groupby("AAA")["BBB"].idxmin()] #idxmin() to get the index of the mins
df.sort_values(by="BBB").groupby("AAA", as_index=False).first() #sort then take first of each

df = df.set_index('row') # As Labelled Index
df.columns = pd.MultiIndex.from_tuples([tuple(c.split('_')) for c in df.columns]) # With Heirarchical Columns
df = df.stack(0).reset_index(1) # Now stack & Reset
df.columns = ['Sample','All_X','All_Y']

#MultiIndexing
stat = df.groupby(by=['code','date'])['close'].max() #return multi-index DataFrame
stat.xs('2016-09-29',level=1) #Slicing a multi-index with xs
All = slice(None)
df.loc[(All,'2016-09-29'),All]
df.loc[(slice('001','601'),'2016-09-29'),All]

#Sort by specific column or an ordered list of columns, with a multi-index
df.sort_values(by=('Labs', 'II'), ascending=False)

#Grouping
gb = df.groupby('date').apply(lambda s: s['code'][s.turnover.idxmax()]) #code of max turnover by day
gb.get_group('2016-09-19')
#gb.apply(fun) #fun(x), x is row
#df.apply(fun,level=0) #level=0 -> column, level=1 -> row 


############ Computational tools ################

df.pct_change() #Percent Change
df.cov() #Covariance
df.corr() #Correlation
df.corrwith() #Correlation like-labeled
df.rank() #rank

#Moving (rolling) statistics / moments
'''
rolling_count Number of non-null observations
rolling_sum Sum of values
rolling_mean Mean of values
rolling_median Arithmetic median of values
rolling_min Minimum
rolling_max Maximum
rolling_std Unbiased standard deviation
rolling_var Unbiased variance
rolling_skew Unbiased skewness (3rd moment)
rolling_kurt Unbiased kurtosis (4th moment)
rolling_quantile Sample quantile (value at %)
rolling_apply Generic apply
rolling_cov Unbiased covariance (binary)
rolling_corr Correlation (binary)
rolling_window Moving window function
'''
pd.rolling_mean(df, window=len(df), min_periods=1)
#Expanding window moment functions
pd.expanding_mean(df)
'''
expanding_count Number of non-null observations
expanding_sum Sum of values
expanding_mean Mean of values
expanding_median Arithmetic median of values
expanding_min Minimum
expanding_max Maximum
expanding_std Unbiased standard deviation
expanding_var Unbiased variance
expanding_skew Unbiased skewness (3rd moment)
expanding_kurt Unbiased kurtosis (4th moment)
expanding_quantile Sample quantile (value at %)
expanding_apply Generic apply
expanding_cov Unbiased covariance (binary)
expanding_corr Correlation (binary)
'''
