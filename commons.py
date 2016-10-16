# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:18:46 2016

@author: q2git
"""

import datetime
import os


TODAY = datetime.date.today()

DATA_DIR =  'data'
STAT_DIR =  'stat'
PLOT_DIR =  'plot'

for folder in [DATA_DIR, STAT_DIR, PLOT_DIR]:
    if not os.path.exists(folder):
        os.mkdir(folder)
 
DB_BASIC = os.path.join(DATA_DIR, 'basics.db') 


if __name__ == '__main__':
    print os.path.join('.',DATA_DIR)
    