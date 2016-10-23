# -*- coding: utf-8 -*-
"""
Created on Sat Oct 15 10:49:28 2016

@author: q2gi
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import datetime
import os


TODAY = datetime.date.today()
PLOT_DIR =  'plot'
if not os.path.exists(PLOT_DIR):
    os.mkdir(PLOT_DIR)

_KWARGS = { #defalut kwargs
    'line':{'kind':'line','subplots':True,'x_compat':True,'marker':'.'},
    'bar':{'kind':'bar',},
    'area':{'kind':'area',},
    }
    
def save_fig(**kwargs):
    
    for k, fig in kwargs.items():
        fig.set_size_inches(16, 9) 
        fn = '{}_{}'.format(TODAY, k)               
        fig.savefig(os.path.join(PLOT_DIR, fn), dpi=200)      

        
def showfig(df, ptype='line', params={}): 
    ptypes = {'line': line_plot,
              'bar': bar_plot,
              'area': area_plot,
              }
              
    msg = ptypes.get(ptype)(df, params) 
    return msg
        
        
def bar_plot(df, params={}):
    try:
        kwargs = {'kind':'bar', 'stacked':True, 'grid':True, 'subplots':False,}
        kwargs2 = {'kind':'line', 'grid':True, 'rot':45, 'marker':'.',
                  'subplots':False, 'legend':False, 
                  'xticks':np.arange(0, len(df), (len(df)/50)+1) }
        if df.columns.size==3:
            kwargs.update({'color':['r','g']})
        kwargs.update(params) # update user kwargs

        nrows = df.columns.size if kwargs.get('subplots',False) else 2

        fig, axes = plt.subplots(nrows=nrows, sharex=True)
        ax = axes[0] if nrows==2 else axes[:-1]
        df.ix[:,:-1].plot(ax=ax,**kwargs)
        df.ix[:,-1].plot(ax=axes[-1], **kwargs2)
        
        for ax in axes:
            ax.legend(loc='upper left', prop={'size':10},)
            
        plt.show()
        return repr(kwargs)
    except Exception as e:
        return 'plot_bar: {}\n'.format(e)          

        
def area_plot(df, params={}):
    try:
        kwargs = {'kind':'area', 'stacked':False, 'grid':True, }
        kwargs2 = {'kind':'line', 'grid':True, 'rot':45, 'marker':'.',
                  'subplots':False, 'legend':False, 
                  'xticks':np.arange(0, len(df), (len(df)/50)+1) }
        if df.columns.size==3:
            kwargs.update({'color':['r','g']})
        kwargs.update(params) # update user kwargs

        fig, axes = plt.subplots(nrows=2, sharex=True)
        
        df.ix[:,:-1].plot(ax=axes[0],**kwargs)
        df.ix[:,-1].plot(ax=axes[-1], **kwargs2)
        
        for ax in axes:
            ax.legend(loc='upper left', prop={'size':10},)
            
        plt.show() 
        return repr(kwargs)
    except Exception as e:
        return 'plot_area: {}\n'.format(e)            


def line_plot(df, params={}):
    try:
        kwargs = {'kind':'line', 'grid':True, 'subplots':False,}
        kwargs2 = {'kind':'line', 'grid':True, 'rot':45, 'marker':'.',
                  'subplots':False, 'legend':False, 
                  'xticks':np.arange(0, len(df), (len(df)/50)+1) }
        if df.columns.size==3:
            kwargs.update({'color':['r','g']})
        kwargs.update(params) # update user kwargs
        
        nrows = df.columns.size if kwargs.get('subplots',False) else 2

        fig, axes = plt.subplots(nrows=nrows, sharex=True)
        ax = axes[0] if nrows==2 else axes[:-1]
        df.ix[:,:-1].plot(ax=ax,**kwargs)
        df.ix[:,-1].plot(ax=axes[-1], **kwargs2)
        
        for ax in axes:
            ax.legend(loc='upper left', prop={'size':10},)
            
        plt.show()
        return repr(kwargs)
    except Exception as e:
        return 'plot_line: {}\n'.format(e)  


def plot1(df, ref='sh'):
    
    print 'Preparing for figures...',

    colnames = df.columns
    #ref = next((x for x in colnames if x.startswith('zs_')))
    ref = 'idx_{}'.format(ref) 
    
    for key in ['pc','over_ma','trend','avg','roll','idx']:
        
        ks = filter(lambda x: x.startswith(key), colnames)
        if key != 'idx': 
            ks.append(ref)

        axes = df[ks].plot(kind='line', grid=True, subplots=True, legend=False, 
                           rot=0, marker='.',)

        fig = plt.gcf() 

        for ax in axes:
            ax.legend(loc='upper left', prop={'size':8},) #, bbox_to_anchor=(1.0, 0.5))
            ax.xaxis.set_minor_locator(mdates.WeekdayLocator(interval=1))           
            ax.xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
            #ax.xaxis.grid(True, which="minor") 
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))            
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))  
            ax.tick_params(axis='both', which='both', labelsize=8)

        plt.tick_params(axis='x', which='minor', labelsize=6, )
        plt.tick_params(axis='x', which='major', pad=20, )        
        plt.setp(axes[-1].xaxis.get_minorticklabels(), rotation=90) 

        fig.set_size_inches(16, 9) 

    print 'Done.'    
    
    return fig
    
if __name__ == '__main__':
    years = raw_input('Years(=2016, 2015.2016): ')
    if not years: years='2016'
    
    idx = raw_input('Index(=sh,sz,hs300,sz50,zxb,cyb): ')
    idx = 'idx_{}'.format(idx) if idx else 'idx_sh'
    
    w = raw_input('Window size(=None): ')
    if w: w=int(w)
    
    from statfuncs import stat
    df = stat(years, w)
    #cols = df.columns.groupby(df.columns.str.slice(0,1))
    cols = df.columns.sort_values().tolist()
    
    for i, col in enumerate(cols): #show columns
        print '{:<3}: {} '.format(i, col)
    
    while 1:
        t = raw_input('please input col ids: ')
        c = raw_input('Total records {}, choose=[0-{}]: '.format(len(df),len(df)))
        c = c.split('-') if c else [0,len(df)-1]
        if t:
            cs = map(lambda x: cols[int(x)], t.split('.'))
            cs.append(idx)
            dfp = df.ix[int(c[0]):int(c[1]), cs]
            dfp.plot(subplots=True, marker='.', 
                    xticks=np.arange(0, len(dfp), (len(dfp)/60)+1), )
            plt.show() 
        else:
            break
    
    raw_input('END')