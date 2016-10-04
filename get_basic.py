# -*- coding: utf-8 -*-
"""
Created on Tue Oct 04 10:50:14 2016

@author: q2git

# get basic data
"""

import tushare as ts
import os


DATA_FOLDER = r'data\basic'
if not os.path.exists(DATA_FOLDER):
    os.mkdir(DATA_FOLDER)

    
def get_basic():
    basics = {
                'basics':ts.get_stock_basics, #股票列表
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
    
    for sht, func in basics.items():
        print 'processing [{}] ...'.format(sht)
        fn = os.path.join(DATA_FOLDER, '{}.csv'.format(sht)) 
        try:
            df = func()
            df.to_csv(fn, encoding='gbk')
        except Exception as e:
            print '[{}]: {}'.format(sht, e)
        print 'done.'

    
if __name__ == '__main__':
    get_basic()
    raw_input('END.') #press any key to exit