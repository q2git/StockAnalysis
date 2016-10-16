# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 12:18:46 2016

@author: q2git
"""

import datetime
import os


TODAY = datetime.date.today()

DATA_DIR =  os.path.join(os.path.abspath('.'), 'data')
STAT_DIR =  os.path.join(os.path.abspath('.'), 'stat')
PLOT_DIR =  os.path.join(os.path.abspath('.'), 'plot')

for folder in [DATA_DIR, STAT_DIR, PLOT_DIR]:
    if not os.path.exists(folder):
        os.mkdir(folder)
 
MSG = '{t:10s}: [{c:6}], [{d:24s}], left: {l:4} ,Msg: {m:2}'


if __name__ == '__main__':
    print PLOT_DIR
    