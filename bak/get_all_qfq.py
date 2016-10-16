# -*- coding: utf-8 -*-
"""
Created on Thu Oct 13 22:09:01 2016

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


DATA_FOLDER = r'data'


class Data_Fetcher(threading.Thread):
    
    def __init__(self, q_code, q_data, lock, year):
        
        threading.Thread.__init__(self)
        self.q_code = q_code
        self.q_data = q_data
        self.lock = lock
        self.year = year
        self.start()
        
    def run(self):
        
        if int(self.year)<=2004:
            start = '2000-01-01'
            end = '2004-12-31'
        else:            
            start = '{}-01-01'.format(self.year)
            end = '{}-12-31'.format(self.year)
        
        with self.lock:
            print '{} has started...'.format(self.getName())
            
        msg = ''
        while 1:
            if self.q_data.qsize() > 10:
                time.sleep(10) #waitting 10s for db writer
            else:
                code = self.q_code.get()
                if code:
                    flg, stk = (True,code[:-4]) if code.endswith('_idx') else (False,code)
                    try:
                        df = ts.get_h_data(code=code, start=start, end=end, 
                                           autype='qfq', #qfq-前复权 hfq-后复权 None-不复权，默认为qfq
                                           index=flg, #是否是大盘指数
                                           drop_factor=False, #不移除复权因子
                                           retry_count=3,pause=3,)                                               
                        df['code'] = code #add column 'code'                      
                        self.q_data.put(df)
                        msg = 'OK'
                        
                    except Exception as e:
                        msg = 'NG'
                        with open('error.txt','a') as f: #error records
                            f.write('Fetcher:{},{}, {}\n'.format(code,self.year,e))                         
                        
                    with self.lock:
                        print '{:10}: [{:6}], left: {:4}, Msg: {}'.format(self.getName(), 
                                code, self.q_code.qsize(), msg) 
                        
                else:
                    self.q_code.put(None) #for other threads to exit
                    break  

        with self.lock:
            print '{} has stopped.'.format(self.getName())


 
def data_writer(db, q_data, lock):
    ''' write data to database ''' 

    print 'Writer has started...' 
    
    with sqlite3.connect(db) as conn:
        conn.text_factory = str
        while 1:
            msg = None
            code = None
            try:
                df = q_data.get()
                if df is None: break
            
                code = df.ix[0,'code']
                table = 'indexs' if code.endswith('_idx') else 'stocks'
                df.to_sql(table, conn, if_exists='append')  
                msg = 'OK'
                
            except Exception as e:
                msg = 'NG'
                with open('error.txt','a') as f: #error records
                    f.write('Writer:{}, {}\n'.format(code, e))  
                    
            with lock:
                print '{:10}: [{:6}], left: {:4} ,Msg: {:2}, DB:{}'\
                        .format('Writer', code, q_data.qsize(), msg, db)            
            
    print 'Writer has stopped.'

           
def main():
    ''' get history data for alll stocks and write it into database '''
    
    year = raw_input('Year=2016: ')
    idx = raw_input('Index only?: ') 
    if not year:
        year = '2016'
    if int(year)<=2004:
        year = '2004'
        
    db = os.path.join(DATA_FOLDER, '{}.db'.format(year))    
    
    print 'Database: [{}]'.format(db) 
    
    n = raw_input('The number of threads (defalut=5): ') 
    n = int(n) if n.isdigit() else 5
    
    q_code = Queue.Queue()
    q_data = Queue.Queue()    
    lock = threading.Lock() 
    
    df = pd.read_excel('codes.xls', converters={'code':str,'date':int})

    codes = ts.get_index().code.add('_idx').tolist()
    
    if not idx:
        codes.extend(df.code[df.date<=int(year)].tolist())
        
    map(q_code.put, codes) #put stock codes and indexs into queue
    q_code.put(None) #end tag

    #fetch threads
    ths_f = []
    for x in xrange(n):
        ths_f.append(Data_Fetcher(q_code, q_data, lock, year))
        
    #write thread 
    #data_writer(db, q_data, lock)                                                
    th_w = threading.Thread(target=data_writer, args=(db, q_data, lock))
    th_w.start() 

    #waiting for fetch threads to exit    
    for th in ths_f:
        th.join()
    
    #exit flag for write thread
    q_data.put(None)
    
    th_w.join()
    
    print 'All tasks have completed!'
 

   
if __name__ == '__main__':
    main()
    raw_input('END.') #press any key to exit
