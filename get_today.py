# -*- coding: utf-8 -*-
"""
Created on Tue Oct 04 10:08:54 2016

@author: q2git

# get today's data and storing it into database
"""

import tushare as ts

import datetime
import os


DATA_FOLDER = r'data\today'
if not os.path.exists(DATA_FOLDER):
    os.mkdir(DATA_FOLDER)

    
def get_today():
    ''' get today's data '''
    print '*'*20,'DAILY TRAING DATA','*'*20
    
    fn = os.path.join(DATA_FOLDER, '{}.csv'.format(datetime.date.today()))
    print 'writting trading data to [{}] ... '.format(fn)
    df = ts.get_today_all() #ts.get_stock_basics()
    df.to_csv(fn, encoding='gbk', ) 
    print 'done.'

    
if __name__ == '__main__':
    get_today()
    raw_input('END.') #press any key to exit