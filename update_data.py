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
import time
import os
from commons import TODAY, DATA_DIR, DB_BASIC


MSG = '{t:10}: [{c:6}], [{d:24}], left: {l:4} ,Msg: {m:2}'


class Data_Fetcher(threading.Thread):
    """ fetching hist data """
    
    def __init__(self, q_code, q_df, lock, event, **kwargs):
        threading.Thread.__init__(self)
        self.q_code = q_code
        self.q_df = q_df
        self.lock = lock
        self.event = event
        self.ktype = kwargs.setdefault('ktype', 'D')
        self.start()
        
    def run(self):
        with self.lock:
            print '{} has started...'.format(self.getName())
            
        msg = ''
        while 1:
            if self.q_df.qsize() > 10:
                self.event.wait(10) #time.sleep(10) #waitting 10s for db writer
            else:
                code, startday = self.q_code.get()
                
                if code and not self.event.is_set():
                    endday = startday.replace(startday[4:],'-12-31')
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
    """ write data to database """

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
                print MSG.format(t='Writer', c=code, d=db[-24:], 
                                 l=q_df.qsize(), m=msg,)            
            
    print 'Writer has stopped.'


def get_codes_with_startday(year, db):
    """ return ((code, startday),...) """
    
    year = int(year)
    #code list
    with sqlite3.connect(DB_BASIC) as con:
        df = pd.read_sql('select code,timeToMarket from codes', con, index_col='code')
        
    df = df['timeToMarket'] / 10000 # convert to year        
    df = df[(df<=(year+1)) & (df>0) ]
  
    codes = ['sh','sz','hs300','sz50','zxb','cyb']#index codes
    codes.extend( df.index.tolist() ) 
    # last update date
    try:
        with sqlite3.connect(db) as con:
            sql='select code,max(date) as lastupdate from stocks group by code'
            sql=sql +' union select code,max(date) from indexs group by code'
            df = pd.read_sql(sql ,con, index_col='code')
            
        df = df['lastupdate']     
        df = pd.to_datetime(df)+pd.DateOffset(1) #add 1 day
        df = df.apply(lambda x:str(x.date())) #to date string
       
    except Exception as e:
        print '{}, use empty DataFrame instead.'.format(e)
        df = pd.DataFrame()
    
    default_startday = '{:4d}-01-01'.format(year)    
    f = lambda x: df.get(x, default_startday)
    codes_sday = ((c,f(c)) for c in codes if f(c)!=str(TODAY) \
                  and int(f(c)[:4])==year)

    return codes_sday    

    
def update_basics():
    """ update basic data """
    
    basics = {
                'codes':ts.get_stock_basics, #股票列表
                'industry':ts.get_industry_classified, #行业分类
                'concept':ts.get_concept_classified, #概念分类
                'area':ts.get_area_classified, #地域分类
                'sme':ts.get_sme_classified, #中小板分类
                'gem':ts.get_gem_classified, #创业板分类
                'st':ts.get_st_classified, #风险警示板分类
                'hs300':ts.get_hs300s, #沪深300成份及权重
                'sz50':ts.get_sz50s, #上证50成份股
                'zz500':ts.get_zz500s, #中证500成份股 
              }
              
    print '\nUpdating [{}]...'.format(DB_BASIC)
      
    with sqlite3.connect(DB_BASIC) as con:
        con.text_factory = str      
        for table, func in basics.items():
            try:
                df = func()
                df.to_sql(table, con, if_exists='replace')
            except Exception as e:
                print table,':-->',e
            
    print 'Done.'
             
           
def update_codes(year, ktype, n):
    """ update stock data """
  
    db = os.path.join(DATA_DIR, '{}_{}.db'.format(year, ktype))    
     
    print '\nUpdating [{}]'.format(db) 
    
    q_code = Queue.Queue()
    q_df = Queue.Queue()    
    lock = threading.Lock()
    event = threading.Event()

    codes_sday = get_codes_with_startday(year, db) 
    #put (code, startday) into queue     
    map(q_code.put, codes_sday) 
    q_code.put((None, None)) #end tag for fetch threads

    #fetch threads
    ths_f = []

    for x in xrange(n):
        ths_f.append(Data_Fetcher(q_code, q_df, lock, event, ktype=ktype))
        
    #write thread                                                 
    th_w = threading.Thread(target=data_writer, args=(db, q_df, lock))
    th_w.start() 

    try:
        while 1:
            if q_code.qsize()<=1:
                break
            else:
                time.sleep(.1)
    except (KeyboardInterrupt, SystemExit):
        print '\n'
        print '#'*5,'Attempting to close threads'.upper(),'#'*5
        event.set()
        
    for th in ths_f: # waitting for all fetch threads to exit
        th.join()
    
    q_df.put(None) # for writer thread to exit
    
    th_w.join()
    
    print 'All tasks have been completed!'
 

def main():
    year = raw_input('Year? [{}]/(y1.y2...) : '.format(TODAY.year))
    year = year if year else str(TODAY.year)
        
    ktype = raw_input('Kteyp? [D]/W/M/5/15/30/60/(k1.k2...) :' ) 
    ktype = ktype.upper() if ktype else 'D'

    n = raw_input('Threads number? [5] : ') 
    n = int(n) if n.isdigit() else 5

    if raw_input('Update basics.db? Y/[N] : '):    
        update_basics()
    
    for y in year.split('.'):
        for k in ktype.split('.'):
            print '\n','#'*10,y,k,n,'#'*10,'\n'
            update_codes(y, k, n)

    
if __name__ == '__main__':
    t1 = time.time()
    main()
    t = (time.time()-t1) / 60
    raw_input('Elapsed time: {} minutes.'.format(t))#press any key to exit
