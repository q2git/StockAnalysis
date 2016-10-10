# -*- coding: utf-8 -*-
"""
Created on Mon Oct 10 09:18:12 2016

@author: q2git
"""

import tushare as ts
import pandas as pd
import sqlite3
import threading
import Queue
import datetime
import time
import os


DATA_FOLDER = r'data\all'
TODAY = datetime.date.today()


class Data_Fetcher(threading.Thread):
    def __init__(self, q_code, q_df, lock, **kwargs):
        threading.Thread.__init__(self)
        self.q_code = q_code
        self.q_df = q_df
        self.lock = lock
        self.ktype = kwargs.setdefault('ktype', 'D')
        self.start = kwargs.setdefault('start', None)
        self.end = kwargs.setdefault('end', None)
        self.start()
        
    def run(self):
        with self.lock:
            print '{} has started...'.format(self.getName())
            
        msg = ''
        while 1:
            if self.q_df.qsize() > 10:
                time.sleep(10) #waitting 10s for db writer
            else:
                code = self.q_code.get()
                if code:
                    try:
                        df = ts.get_hist_data(code, start=self.start,
                                              end=self.end,
                                              ktype=self.ktype,
                                              retry_count=3, pause=3)
                                              
                        df['code'] = code #add column 'code'                      
                        self.q_df.put(df)
                        msg = 'OK'
                    except Exception as e:
                        msg = 'NG-{}'.format(e.message)
                        
                    with self.lock:
                        print '{:10}: [{}], left: {:5}, Msg: {}'.format(self.getName(), 
                                code, self.q_code.qsize(), msg)        
                        with open('error.txt','a') as f: #error records
                            f.write('{},error: {}\n'.format(code, e))                        
                        
                else:
                    self.q_code.put(None) #for other threads to exit
                    break  

        with self.lock:
            print '{} has stopped.'.format(self.getName())


 
def data_writer(conn, q_df, lock):
    ''' write data to database ''' 
    print 'Writer has started...'    
    while 1:
        data = q_df.get()
        if data:
            code, df = data.code[0], data
            table = 'stocks' if code.isdigit() else 'indexs'
            df.to_sql(table, conn, if_exists='append')  
            with lock:
                print 'Writer : [{}], left: {}'.format(code, q_df.qsize())
        else:
            break
    print 'Writer has stopped.'



def get_code_list():
    codes = ts.get_stock_basics().index.tolist() #stock codes 
    codes.extend(['sh','sz','hs300','sz50','zxb','cyb',]) #index codes
    return codes
    

           
def main():
    ''' get history data for alll stocks and write it into database '''
    
    #year = raw_input('Year: ')
    #if not year.isdigit(): 
    year = TODAY.year
        
    db = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, 'D'))  
    if os.path.isfile(db):
        print 'The file [{}] has already existed.'.format(db)
        return   
    
    conn = sqlite3.connect(db)
    conn.text_factory = str
    
    lastupdate = conn.execute('select date from indexs order by date desc').fetchone()
    start = pd.to_datetime(lastupdate) + pd.DateOffset(1) #add 1 day
    
    print 'Ready to update [{}], start day: [{}]'.format(db, start) 
    raw_input('Press any key to continue...') 
    
    q_code = Queue.Queue()
    q_df = Queue.Queue()    
    lock = threading.Lock() 
    
    codes = get_code_list()    
    map(q_code.put, codes) #put stock codes and indexs into queue

    #fetch threads
    for x in xrange(5):
        Data_Fetcher(q_code, q_df, lock, start=start)
        
    #write thread                                                 
    th_w = threading.Thread(target=data_writer, args=(conn, q_df, lock))
    th_w.start() 
    th_w.join()
    
    print 'All tasks have completed!'
 

   
if __name__ == '__main__':
    main()
    raw_input('END.') #press any key to exit
