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
        self.startday = kwargs.setdefault('start', None)
        self.endday = kwargs.setdefault('end', None)
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
                        df = ts.get_hist_data(code, start=self.startday,
                                              end=self.endday,
                                              ktype=self.ktype,
                                              retry_count=3, pause=3)
                                              
                        df['code'] = code #add column 'code'                      
                        self.q_df.put(df)
                        msg = 'OK'
                    except Exception as e:
                        msg = 'NG'
                        with open('error.txt','a') as f: #error records
                            f.write('Fetcher:{}, {}\n'.format(code, e))                         
                        
                    with self.lock:
                        print '{:10}: [{:6}], left: {:4}, Msg: {}'.format(self.getName(), 
                                code, self.q_code.qsize(), msg)        
                       
                        
                else:
                    self.q_code.put(None) #for other threads to exit
                    break  

        with self.lock:
            print '{} has stopped.'.format(self.getName())


 
def data_writer(db, q_df, lock):
    ''' write data to database ''' 
	#TBD: writer cannot exit
    print 'Writer has started...' 
    with sqlite3.connect(db) as conn:
        conn.text_factory = str
        while 1:
            msg = None
            code = None
            try:
                df = q_df.get()
                if df is None: break
            
                code = df.ix[0,'code']
                table = 'stocks' if code.isdigit() else 'indexs'
                df.to_sql(table, conn, if_exists='append')  
                msg = 'OK'
            except Exception as e:
                msg = 'NG'
                with open('error.txt','a') as f: #error records
                    f.write('Writer:{}, {}\n'.format(code, e))  
                    
            with lock:
                print '{:10}: [{:6}], left: {:4} ,Msg: {:2}, DB:{}'\
                        .format('Writer', code, q_df.qsize(), msg, db)            
            
    print 'Writer has stopped.'



def get_code_list():
    #codes = ts.get_stock_basics().index.tolist() #stock codes 
    #codes = ['600596','000009','300331','000001','000002'] 
    codes = ts.get_today_all().code.tolist()
    codes.extend(['sh','sz','hs300','sz50','zxb','cyb',]) #index codes

    return codes
    

           
def main():
    ''' get history data for alll stocks and write it into database '''
    
    #year = raw_input('Year: ')
    #if not year.isdigit(): 
    year = TODAY.year
        
    db = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, 'D'))    
    
    with sqlite3.connect(db) as conn:
        rs = conn.execute('select date from indexs order by date desc').fetchone()
    
    t = pd.to_datetime(rs[0]) + pd.DateOffset(1) #add 1 day
    start = str(t.date())
    
    print 'Ready to update [{}], start day: [{}]'.format(db, start) 
    n = raw_input('The number of threads (defalut=5): ') 
    n = int(n) if n.isdigit() else 5
    
    q_code = Queue.Queue()
    q_df = Queue.Queue()    
    lock = threading.Lock() 
    
    codes = get_code_list()    
    map(q_code.put, codes) #put stock codes and indexs into queue
    q_code.put(None) #end tag

    #fetch threads
    for x in xrange(n):
        Data_Fetcher(q_code, q_df, lock, start=start)
        
    #write thread                                                 
    th_w = threading.Thread(target=data_writer, args=(db, q_df, lock))
    th_w.start() 
    th_w.join()
    
    print 'All tasks have completed!'
 

   
if __name__ == '__main__':
    main()
    raw_input('END.') #press any key to exit
