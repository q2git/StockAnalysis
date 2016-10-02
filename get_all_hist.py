# -*- coding: utf-8 -*-
"""
Created on Thu Sep 29 18:51:16 2016

@author: q2git

# get all stocks data from internet and store it into database
"""

import tushare as ts
import sqlite3
import threading
import Queue
import time
import os

DATA_FOLDER = r'data\db'
# k line type         
global KTYPE, DB_PATH

def code_to_que(q_code):
    ''' put code into queue '''
    codes = ts.get_stock_basics().index.tolist() #stock codes 
    codes.extend(['sh','sz','hs300','sz50','zxb','cyb',]) #index codes
    map(q_code.put, codes)
    q_code.put(None) #end tag
    

def fetch_hist_data(th, year, q_code, q_data, lock): 
    ''' get data and put it into queue, ktype=D '''
    global KTYPE
    
    if year=='all':
        start=None
        end=None
    else:
        start = '{}-01-01'.format(year)
        end = '{}-12-31'.format(year)
    print 'Fetcher-{} has started...'.format(th)
    while 1:
        if q_data.qsize() > 10:
            time.sleep(10) #waitting for Writer
        else:
            code = q_code.get()
            if code:
                try:
                    df = ts.get_hist_data(code, start=start, 
                                          end=end, ktype=KTYPE,
                                          retry_count=3, pause=3)
                    df['code'] = code #add column 'code'                      
                    q_data.put((code,df))
                    with lock:
                        print 'Fetcher-{}: [{}], left: {}'.format(th, code,q_code.qsize())
                
                except Exception as e:
                    with open('error.txt','a') as f: #error records
                        f.write('{},error: {}\n'.format(code, e))
    
            else:
                q_code.put(None) #for other threads to exit
                break
        
            
    q_data.put(None) #finish tag
    print 'Fetcher-{} has stopped.'.format(th)
 
 
def write_hist_data(year, q_data, lock):
    ''' write data to database '''
    global KTYPE, DB_PATH
    
    conn = sqlite3.connect(DB_PATH)
    conn.text_factory = str     
    print 'Writer has started...'
    while 1:
        data = q_data.get()
        if data:
            code, df = data
            table = 'stocks' if code.isdigit() else 'indexs'
            df.to_sql(table, conn, if_exists='append')  
            with lock:
                print 'Writer   : [{}] to [{}], left: {}'.format(code, DB_PATH, q_data.qsize())
        else:
            break
    print 'Writer has stopped.'

            
def get_all():
    ''' get history data for alll stocks and write it into database '''
    
    year = raw_input('Year or all: ')
    if not(year.isdigit()) and not(year=='all'): 
        print 'Wrong input.'
        return
    
    global KTYPE, DB_PATH
    ktype = str(raw_input('Ktype=D (W,M,5,15,30,60): ')).upper()
    if ktype in ['W','M','5','15','30','60']:
        KTYPE = ktype
    else:
        KTYPE = 'D'
        
    DB_PATH = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, KTYPE))
    if os.path.isfile(DB_PATH):
        print 'The file [{}] has already existed.'.format(DB_PATH)
        return
        
    q_code = Queue.Queue()
    q_data = Queue.Queue()    
    lock = threading.Lock() 
    code_to_que(q_code) #put stock codes and indexs into queue
    #fetch threads
    th_r = []
    for x in xrange(5):
        th = threading.Thread(target=fetch_hist_data, 
                            args=(x, year, q_code, q_data, lock))        
        th.start()
        th_r.append(th)
    #write thread                                                 
    th_w = threading.Thread(target=write_hist_data, args=(year, q_data, lock))
    th_w.start() 
    
    for th in th_r:
        th.join() 
    th_w.join()
    print 'All tasks have completed!'
    
if __name__ == '__main__':
    #hists(2015)
    get_all()
    raw_input('END.') #press any key to exit