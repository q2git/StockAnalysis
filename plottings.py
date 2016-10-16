# -*- coding: utf-8 -*-
"""
Created on Sat Oct 15 10:49:28 2016

@author: q2gi
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from commons import TODAY, PLOT_DIR


def save_fig(**kwargs):
    
    for k, fig in kwargs.items():
        fig.set_size_inches(16, 9) 
        fn = '{}_{}'.format(TODAY, k)               
        fig.savefig(os.path.join(PLOT_DIR, fn), dpi=200)     
 

   
def plot_pair(df, ref='sh'):
    
    print 'Preparing for figures...',
    figs = {}
    cols = df.columns.sort_values()
    ref = 'idx_{}'.format(ref) 
    prefixs = [ 'trend', 'pc', 'roll',] #paired params 
                #'over_ma', 'avg', 'idx' ]

    keys_group = map(lambda y:filter(lambda x: x.startswith(y), cols,),prefixs)
    
    for ks in keys_group:
        pairs = zip(ks[:len(ks)/2], ks[len(ks)/2:])
        fig, axes = plt.subplots(nrows=len(pairs), sharex=True)
        
        for keys, ax in zip(pairs,axes):
            ks = [keys[0], keys[1], ref]
            
            df[ks].plot(kind='line', grid=True, subplots=False, legend=True, 
                    rot=0, marker='.', ax=ax, secondary_y=[ref], color=['r','g','b'])

            ax.legend(loc='upper left', prop={'size':8},)          
            
        #df[ref].plot(kind='line', grid=True, subplots=False, legend=True, 
        #                   rot=0, marker='.', ax=axes[-1], )
                           
        axes[-1].tick_params(axis='x', which='minor', labelsize=8, )
        axes[-1].tick_params(axis='x', which='major', pad=20, )  
        axes[-1].xaxis.set_minor_locator(mdates.WeekdayLocator(interval=1))           
        axes[-1].xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
        #axes[-1].xaxis.grid(True, which="minor") 
        axes[-1].xaxis.set_major_locator(mdates.MonthLocator(interval=1))            
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))           
        plt.setp(axes[-1].xaxis.get_minorticklabels(), rotation=90) 
        
        figs['{}'.format(keys)] = fig
    print 'Done.' 
    
    return figs


def plot_line(df, ref='sh'):
    
    print 'Preparing for figures...',
    figs = {}
    cols = df.columns.sort_values()
    ref = 'idx_{}'.format(ref) 
    prefixs = ['over_ma', 'avg', 'idx' ]

    for prefix in prefixs:
        ks = filter(lambda x: x.startswith(prefix), cols)        
        if prefix!='idx':
            ks.append(ref)
        
        axes = [df[ks].plot(kind='line', grid=True, subplots=False, legend=True, 
                rot=0, marker='.', secondary_y=[ref],)]
                           
        axes[-1].tick_params(axis='x', which='minor', labelsize=8, )
        axes[-1].tick_params(axis='x', which='major', pad=20, )  
        axes[-1].xaxis.set_minor_locator(mdates.WeekdayLocator(interval=1))           
        axes[-1].xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
        #axes[-1].xaxis.grid(True, which="minor") 
        axes[-1].xaxis.set_major_locator(mdates.MonthLocator(interval=1))            
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))           
        plt.setp(axes[-1].xaxis.get_minorticklabels(), rotation=90) 
        
        fig = plt.gcf()
        figs['{}'.format(prefix)] = fig
    print 'Done.' 
    
    return figs