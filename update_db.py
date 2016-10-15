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
MSG = '{t:10}: [{c:6}], [{d:24}], left: {l:4} ,Msg: {m:2}'

class Data_Fetcher(threading.Thread):
    def __init__(self, q_code, q_df, lock, **kwargs):
        threading.Thread.__init__(self)
        self.q_code = q_code
        self.q_df = q_df
        self.lock = lock
        self.ktype = kwargs.setdefault('ktype', 'D')
        self.start()
        
    def run(self):
        with self.lock:
            print '{} has started...'.format(self.getName())
            
        msg = ''
        while 1:
            if self.q_df.qsize() > 10:
                time.sleep(10) #waitting 10s for db writer
            else:
                code, startday = self.q_code.get()
                if code:
                    endday = startday.replace(startday[5:],'12-31')
                    try:
                        df = ts.get_hist_data(code, start=startday,
                                              end=endday,
                                              ktype=self.ktype,
                                              retry_count=3, pause=3)
                        #in case of no data                      
                        if isinstance(df, pd.DataFrame) and not df.empty: 
                            df['code'] = code #add column 'code'                         
                            self.q_df.put(df)
                            msg = 'OK'
                        else:
                            msg = 'No Data'
                            
                    except Exception as e:
                        msg = 'NG'
                        with open('error.txt','a') as f: #error records
                            f.write('Fetcher:{}, {}\n'.format(code, e))                         
                        
                    with self.lock:
                        print MSG.format(t=self.getName(), c=code, m=msg, 
                            d=startday+' to '+endday, l=self.q_code.qsize())        
                       
                        
                else:
                    self.q_code.put((None, None)) #for other threads to exit
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
                print MSG.format(t='Writer', c=code, d=db, 
                                 l=q_df.qsize(), m=msg,)            
            
    print 'Writer has stopped.'



def get_codes_with_startday(year, db):
    ''' return ((code, startday),...) '''
    
    try: # code list with time to market
        df = ts.get_stock_basics()
        cols = ['name','industry','area']
        df[cols] = df[cols].applymap(lambda x: x.decode('utf8'))
        df.to_excel('codes.xlsx')  
        
    except Exception as e:
        print '{}, use codes.xlsx instead.'.format(e) 
        df = pd.read_excel('codes.xlsx', index_col='code', parse_cols='A,P',)
        df.index = map(lambda x:'{:06d}'.format(x), df.index) 
        
    df = df['timeToMarket'] / 10000 # convert to year        
    df = df[(df<=(int(year)+1)) & (df>0) ]

    codes = df.index.tolist()
    #codes = ['600596','000009','300331','000001','000002']     
    codes.extend(['sh','sz','hs300','sz50','zxb','cyb',]) #index codes

    # last update date
    try:
        with sqlite3.connect(db) as con:
            sql = 'select code,max(date) as lastupdate from stocks group by code'
            df = pd.read_sql(sql ,con, index_col='code')
            
        df = df['lastupdate']     
        df = pd.to_datetime(df)+pd.DateOffset(1) #add 1 day
        df = df.apply(lambda x:str(x.date())) #to date string
       
    except Exception as e:
        print '{}, use empty DataFrame instead.'.format(e)
        df = pd.DataFrame()
    
    default_startday = '{}-01-01'.format(year)    
    f = lambda x: df.get(x, default_startday)
    codes_sday = ((c,f(c)) for c in codes if f(c)!=str(TODAY)) 

    return codes_sday
    

           
def main():
    ''' get history data for alll stocks and write it into database '''
    
    year = raw_input('Year(default={}) : '.format(TODAY.year))
    if not year.isdigit(): 
        year = TODAY.year
        
    db = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, 'D'))    
     
    print 'Ready to update [{}]'.format(db) 
    n = raw_input('The number of threads (defalut=5): ') 
    n = int(n) if n.isdigit() else 5
    
    q_code = Queue.Queue()
    q_df = Queue.Queue()    
    lock = threading.Lock() 

    codes_sday = get_codes_with_startday(year, db) 
    #put (code, startday) into queue     
    map(q_code.put, codes_sday) 
    q_code.put((None, None)) #end tag for fetch threads

    #fetch threads
    ths_f = []
    for x in xrange(n):
        ths_f.append(Data_Fetcher(q_code, q_df, lock))
        
    #write thread                                                 
    th_w = threading.Thread(target=data_writer, args=(db, q_df, lock))
    th_w.start() 

    for th in ths_f: # waitting for all fetch threads to exit
        th.join()
    
    q_df.put(None) # for writer thread to exit
    
    th_w.join()
    
    print 'All tasks have completed!'
 

   
if __name__ == '__main__':
    t1 = time.time()
    main()
    t = (time.time()-t1) / 60
    raw_input('Elapsed time: {} minutes.'.format(t))#press any key to exit
