﻿# -*- coding: utf-8 -*-
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
import datetime
import os


TODAY = datetime.date.today()
DATA_DIR =  'data'
if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR) 
DB_BASIC = os.path.join(DATA_DIR, 'basics.db') 

MSG = '{t:10}: [{c:6}], [{d:24}], left: {l:4} ,Msg: {m:2}'


class Data_Fetcher(threading.Thread):
    """ fetching hist data """
    
    def __init__(self, q_code, q_df, lock, event, ktype):
        threading.Thread.__init__(self)
        self.q_code = q_code
        self.q_df = q_df
        self.lock = lock
        self.event = event
        self.ktype = ktype
        self.start()
        
    def run(self):
        with self.lock:
            print '{} has started...'.format(self.getName())

        while 1:
            msg = 'No Data'
            if self.event.is_set(): break
            try:
                if self.q_df.qsize() > 20: 
                    self.event.wait(10) #time.sleep(10) #waitting 10s for db writer 
                    continue
                _code = self.q_code.get()
                if _code:
                    code, startday = _code
                    endday = startday.replace(startday[4:],'-12-31')
                    if self.ktype=='QFQ':
                        _idx = True if code[-1]=='S' else False
                        _stk = code[:-1] if _idx else code
                        #sys.stdout = open(os.devnull, "w") # disable printing
                        df = ts.get_h_data(code=_stk, start=startday, 
                             end=endday, index=_idx, drop_factor=False) 
                        #sys.stdout = open(os.devnull, "w") # disable printing  
                    else:
                        df = ts.get_hist_data(code, start=startday,
                             end=endday, ktype=self.ktype,
                             retry_count=3, pause=3)
                    #in case of no data                      
                    if isinstance(df, pd.DataFrame) and not df.empty: 
                        df['code'] = code #add column 'code'                         
                        self.q_df.put(df)
                        msg = 'OK'
                else:
                    self.q_code.put(None) #for other threads to exit                     
                    break 
                  
            except Exception as e:
                msg = '{}'.format(e) 
                with open('error.txt','a') as f: #error records
                    f.write('Fetcher:{}, {}\n'.format(code, msg))                 
            
            with self.lock:
                print MSG.format(t=self.getName(), c=code, m=msg, 
                                 d=startday+' to '+endday, l=self.q_code.qsize())                                             
                
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
                if isinstance(df, pd.DataFrame): 
                    code = df.ix[0,'code']
                    table = 'stocks' if code.isdigit() else 'indexs'
                    df.to_sql(table, conn, if_exists='append')  
                    msg = 'OK'
                else:
                    break
            except Exception as e:
                msg = 'NG'
                with open('error.txt','a') as f: #error records
                    f.write('Writer:{}, {}\n'.format(code, e))  
                    
            with lock:
                print MSG.format(t='Writer', c=code, d=db[-24:], 
                                 l=q_df.qsize(), m=msg,)            
            
    print 'Writer has stopped.'


def get_codes_with_startday(year, db, qfq=False):
    """ return ((code, startday),...) """
    
    year = int(year)
    #code list
    with sqlite3.connect(DB_BASIC) as con:
        df = pd.read_sql('select code,timeToMarket from codes', con, index_col='code')
        
    df = df['timeToMarket'] / 10000 # convert to year        
    df = df[(df<=(year+1)) & (df>0) ]
    # add indexs
    if qfq:
        codes = ts.get_index().code.add('S').tolist() #distinguish from stk code
    else:
        codes = ['sh','sz','hs300','sz50','zxb','cyb']#index codes
    codes.extend( df.index.tolist() ) 
    # last update date
    try:
        with sqlite3.connect(db) as con:
            sql1='select code,max(date) as lastupdate from indexs group by code'
            sql2=sql1 +' union select code,max(date) from stocks group by code'
            try:
                df = pd.read_sql(sql2 ,con, index_col='code')
            except:
                df = pd.read_sql(sql1 ,con, index_col='code')
            
        df = df['lastupdate']     
        df = pd.to_datetime(df)+pd.DateOffset(1) #add 1 day
        df = df.apply(lambda x:str(x.date())) #to date string
       
    except Exception as e:
        print '{}, use empty DataFrame instead.'.format(e)
        df = pd.DataFrame()
    
    _yearstart = '{:4d}-01-01'.format(year) 
    _yearend = '{:4d}-12-31'.format(year)      
    f = lambda x: df.get(x, _yearstart)
    codes_sday = (  (c,f(c)) for c in codes if f(c)<=str(TODAY) \
                    and int(f(c)[:4])==year and f(c)!=_yearend  )

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
                if isinstance(df, pd.DataFrame):
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

    codes_sday = get_codes_with_startday(year, db, ktype=='QFQ') 
    #put (code, startday) into queue     
    map(q_code.put, codes_sday) 
    q_code.put(None) #end tag for fetch threads

    #write thread                                                 
    th_w = threading.Thread(target=data_writer, args=(db, q_df, lock))
    th_w.start() 

    #fetch threads
    ths_f = []    
    for x in xrange(n):
        ths_f.append(Data_Fetcher(q_code, q_df, lock, event, ktype))
        time.sleep(3)   

    try:
        while 1:
            if q_code.qsize()<=1:
                print 'q_code is empty.' 
                break
            else:
                time.sleep(.1)
    except (KeyboardInterrupt, SystemExit):
        print '\n'
        print '#'*5,'Attempting to close threads'.upper(),'#'*5
    
    event.set()
    
    if ktype=='QFQ': #fetch thread may dead for getting QFQ data
        print 'Waitting 60s, then ignore fetch threads.'
        time.sleep(60)
    else:
        for th in ths_f: # waitting for all fetch threads to exit
            th.join()
        print 'All fetch threads have stopped.'
    
    q_df.put(None) # for writer thread to exit
    th_w.join()
    print 'All tasks have been completed!'
 

def main():
    year = raw_input('Year? [{}]/(y1.y2...) : '.format(TODAY.year))
    year = year if year else str(TODAY.year)
        
    ktype = raw_input('Ktype? [D]/W/M/5/15/30/60/(k1.k2...)/q=QFQ :' ) 
    if ktype.upper().startswith('Q'):
        ktype = 'QFQ'
    else:
        ktype = ktype.upper() if ktype else 'D'     

    n = raw_input('Threads number? [5] : ') 
    n = int(n) if n.isdigit() else 5

    if raw_input('Update basics.db? Y/[N] : '):    
        update_basics()
    
    for y in year.split('.'):
        for k in ktype.split('.'):
            print '\n{} Year={}, Ktype={}, Threads={} {}\n'\
                    .format('#'*10,y,k,n,'#'*10)
            update_codes(y, k, n)
    
    print 'Get today all and save it into excel.'
    if raw_input('Get today all? Y/[N]'):
        df = ts.get_today_all() #just for record
        df.to_excel(os.path.join(DATA_DIR, 'today_all_{}.xlsx'.format(TODAY)))
    
    if raw_input('Do it again? Y/[N]'):
        main()

        
if __name__ == '__main__':
    t1 = time.time()
    main()
    t = (time.time()-t1) / 60
    raw_input('Elapsed time: {} minutes.'.format(t))#press any key to exit
