# -*- coding: utf-8 -*-
"""
Created on Sat Oct 01 16:49:09 2016

@author: q2git
"""

import tushare as ts


'''
交易数据 http://tushare.org/trading.html
'''
#历史行情
ts.get_hist_data('600848') #一次性获取全部数据
ts.get_hist_data(code='600596', #'sh','sz','hs300','sz50','zxb','cyb',index
                 start='2016-01-01',end='2016-12-31',
                 ktype='D', #D,W,M,5,15,30,60，默认为D
                 retry_count=3,pause=1,)
#复权数据
ts.get_h_data(code='600596',start='2016-01-01',end='2016-12-31', 
              autype='qfq', #qfq-前复权 hfq-后复权 None-不复权，默认为qfq
              index=False, #是否是大盘指数
              retry_count=3,pause=1,)             
#实时行情
ts.get_today_all()
#历史分笔
ts.get_tick_data(code='600596',date='2016-01-01',retry_count=3,pause=1,)                
#实时分笔
#6位数字股票代码，或者指数代码 (sh=上证指数 sz=深圳成指 hs300=沪深300指数 
#sz50=上证50 zxb=中小板 cyb=创业板) 可输入的类型：str、list、set
#或者pandas的Series对象                
ts.get_realtime_quotes('600596') 
ts.get_realtime_quotes(['600848','000980','sh'])                
#当日历史分笔
ts.get_today_ticks(code='601333',retry_count=3,pause=1,)                      
#大盘指数行情列表
ts.get_index()  
#大单交易数据
ts.get_sina_dd(code='600596', date='2016-09-09',
               vol=400, #默认为400手，输入数值型参数
               retry_count=3,pause=1,)       


''' 
投资参考数据 http://tushare.org/reference.html 
'''
#分配预案
ts.profit_data(year=2016, #默认为2014
               top=25, #默认取最近公布的25条
               retry_count=3,pause=1,)               
#业绩预告
ts.forecast_data(year=2016,quarter=1) #季度 :1、2、3、4，只能输入这4个季度                                                  
#限售股解禁 
ts.xsg_data(year=2016, #默认为当前年
            month=1, #解禁月份, 默认为当前月
            retry_count=3,pause=1,)
#基金持股
ts.fund_holdings(year=2016,quarter=1,retry_count=3,pause=1,)                 
#融资融券（沪市）
ts.sh_margins(start='2015-01-01', #为空时取去年今日
              end='2015-04-19', #为空时取当前日期
              retry_count=3,pause=1,)                 
#融资融券（深市）
ts.sz_margins(start='2015-01-01', #为空时取去年今日
              end='2015-04-19', #为空时取当前日期
              retry_count=3,pause=1,)   
#沪市融资融券明细数据
ts.sh_margin_details(#date='2015-09-09', # 默认为空’‘,数据返回最近交易日明细数据
                     start='2015-01-01', end='2015-04-19',symbol='601989',
                     retry_count=3,puase=1,)                    
#深市融资融券明细数据
#深市融资融券明细一次只能获取一天的明细数据，如果不输入参数，则为最近一个交易日的明细数据
ts.sz_margin_details('2015-04-20')

               
''' 
股票分类数据 http://tushare.org/classifying.html 
'''
ts.get_industry_classified() #行业分类
ts.get_concept_classified() #概念分类
ts.get_area_classified() #地域分类
ts.get_sme_classified() #中小板分类
ts.get_gem_classified() #创业板分类
ts.get_st_classified() #风险警示板分类
ts.get_hs300s() #沪深300成份及权重
ts.get_sz50s() #上证50成份股
ts.get_zz500s() #中证500成份股


'''
基本面数据 http://tushare.org/fundamental.html
'''
ts.get_stock_basics() #股票列表
ts.get_report_data(2014,3) #业绩报告（主表） #获取2014年第3季度的业绩报表数据
ts.get_profit_data(2014,3) #盈利能力
ts.get_operation_data(2014,3) #营运能力
ts.get_growth_data(2014,3) #成长能力
ts.get_debtpaying_data(2014,3) #偿债能力
ts.get_cashflow_data(2014,3) #现金流量


'''
宏观经济数据
'''
ts.get_deposit_rate() #存款利率
ts.get_loan_rate() #贷款利率
ts.get_rrr() #存款准备金率
ts.get_money_supply() #货币供应量
ts.get_money_supply_bal() #货币供应量(年底余额)
ts.get_gdp_year() #国内生产总值(年度)
ts.get_gdp_quarter() #国内生产总值(季度)
ts.get_gdp_for() #三大需求对GDP贡献
ts.get_gdp_pull() #三大产业对GDP拉动
ts.get_gdp_contrib() #三大产业贡献率
ts.get_cpi() #居民消费价格指数
ts.get_ppi() #工业品出厂价格指数


'''
新闻事件数据
'''
ts.get_latest_news() #默认获取最近80条新闻数据，只提供新闻类型、链接和标题
ts.get_latest_news(top=5,show_content=True) #显示最新5条新闻，并打印出新闻内容
ts.get_notices() #信息地雷 如果获取信息内容，请调用notice_content(url)接口，把url地址作为参数传入即可。
ts.guba_sina() #新浪股吧


'''
龙虎榜数据
'''
ts.top_list('2016-06-12') #每日龙虎榜列表
ts.cap_tops() #个股上榜统计
ts.broker_tops() #营业部上榜统计
ts.inst_tops() #机构席位追踪
ts.inst_detail() #机构成交明细


'''
银行间同业拆放利率
'''
#Shibor拆放利率
df = ts.shibor_data() #取当前年份的数据
#df = ts.shibor_data(2014) #取2014年的数据
df.sort('date', ascending=False).head(10)

#贷款基础利率（LPR）
df = ts.lpr_data() #取当前年份的数据
#df = ts.lpr_data(2014) #取2014年的数据
df.sort('date', ascending=False).head(10)


'''
数据存储
'''
#直接保存
df.to_csv('c:/day/000875.csv')
#选择保存
df.to_csv('c:/day/000875.csv',columns=['open','high','low','close'])
#追加数据的方式
import os
filename = 'c:/day/bigfile.csv'
for code in ['000875', '600848', '000981']:
    df = ts.get_hist_data(code)
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=None)
    else:
        df.to_csv(filename)

#直接保存
df.to_excel('c:/day/000875.xlsx')
#设定数据位置（从第3行，第6列开始插入数据）
df.to_excel('c:/day/000875.xlsx', startrow=2,startcol=5)

df.to_hdf('c:/day/hdf.h5','000875') #方法1
#store = HDFStore('c:/day/store.h5') #方法2
#store['000875'] = df
#store.close()

df.to_json('c:/day/000875.json',orient='records')
#或者直接使用
print df.to_json(orient='records')

'''
from sqlalchemy import create_engine
import tushare as ts
df = ts.get_tick_data('600848', date='2014-12-22')
engine = create_engine('mysql://user:passwd@127.0.0.1/db_name?charset=utf8')
#存入数据库
df.to_sql('tick_data',engine)
#追加数据到现有表
#df.to_sql('tick_data',engine,if_exists='append')

import pymongo
import json
conn = pymongo.Connection('127.0.0.1', port=27017)
df = ts.get_tick_data('600848',date='2014-12-22')
conn.db.tickdata.insert(json.loads(df.to_json(orient='records')))
'''
