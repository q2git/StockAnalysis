# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 10:56:55 2016
@author: q2git
"""

from tkfactory import TkFactory
from statfuncs import stat, df_idxs_codes
import numpy as np
import matplotlib.pyplot as plt
import threading
import Queue

_KWARGS = { #defalut kwargs
    'line':{'kind':'line','subplots':True,'x_compat':True,'marker':'.'},
    'bar':{'kind':'bar',},
    'area':{'kind':'area',},
    }
            
class Gui(TkFactory):
    def __init__(self, filename, master=None):
        TkFactory.__init__(self, filename, master)
        self.que = Queue.Queue()
        self.config_cmd()
        
    def config_cmd(self):
        #bind_all
        self.bind_all('<Control-KeyPress-q>', self.stop)
        self.bt1.config(command= self.load_stat)        
        self.bt2.config(command= self.send2ref)  
        self.bt3.config(command= self.showfigs)
        self.bt_stat.config(command= self.stat_data)        
        self.list1.bind('<Double-1>', self.add2plot)
        self.list1.bind('<3>', self.send2plot) 
        self.list2.bind('<Double-1>', 
                        lambda _:self.list2.delete(self.list2.curselection()[0]))
        self.group1.bind("<<ComboboxSelected>>", self.setlist1)        
        self.startday.bind("<<ComboboxSelected>>", self.set_enddays)
        self.bt_bar.config(command= lambda: self.showfigs('bar'))
        self.bt_area.config(command= lambda: self.showfigs('area'))
        self.kwargs.insert('1.0', '{   }')
        self.kwargs.insert('end', '\n\n'+'\n\n'.join(map(repr, _KWARGS.values())))
        
    def check_que(self):
        try:
            flag, data = self.que.get_nowait()
            if flag==1:
                self.df = data #stat results
                self.setbydf()
            elif flag==2:
                self.dfi, self.dfc = data #idxs, codes
                self.stat_data()
            else:
                self.txt1.insert('end', data)
                self.after(1000, self.check_que)                
        except Queue.Empty:
            self.txt1.insert('end', '.') 
            self.after(1000, self.check_que)
        #else:
            #self.after(1000, self.check_que)
        
    def load_stat(self):
        self.bt1.var.set('Loading...')
        try:
            years = self.years.var.get()
            ktype = self.ktype.var.get()
            mas = map(lambda x: int(x), self.mas.get().split(','))
            rmxx = map(lambda x: int(x), self.rmxx.get().split(','))
            #self.df = stat(years, ktype, window, mas, rmxx)
            threading.Thread(target=df_idxs_codes, args=(years, ktype, 
                                mas, rmxx, self.que)).start()
            self.after(1000, self.check_que)
        except Exception as e:
            print e

    def stat_data(self):
        w = int(self.window.get())
        k = self.group.get()                
        threading.Thread(target=stat, 
                         args=(self.dfi, self.dfc, w, k, self.que)).start() # stat thread        
        self.after(1000, self.check_que) 
         
    def setbydf(self):
        self.bt1.var.set('Load & Stat & Plot')        
        days = self.df.index.tolist() 
        self.startday.config(values=days)
        self.startday.var.set(days[0])
        self.endday.var.set(days[-1])  
        self.setlist1(flag=True)

    def setlist1(self, event=None, flag=False):
        cols =  self.df.columns.sort_values()          
        dict_cols = cols.groupby(cols.str.extract('(^.+:)', expand=False))
        if flag: 
            gp = sorted(dict_cols.keys())
            self.group.config(values= gp)  
            gp.append('All Parameters')                  
            self.group1.config(values= gp)  
        k = self.group1.get()        
        vals = dict_cols.get(k, [i for s in dict_cols.values() for i in s])
        self.list1.listvar.set(tuple(vals))
        
    def send2plot(self, event=None):
        idxs = self.list1.curselection()
        vals = tuple(map(self.list1.get, idxs))
        self.list2.listvar.set(vals)

    def add2plot(self, event=None):
        idx = self.list1.curselection()[0]
        self.list2.insert('end', self.list1.get(idx))
        
    def send2ref(self, event=None):
        idx = self.list1.curselection()[0]
        val = self.list1.get(idx)
        self.ref.var.set(val)

    def get_plot_df(self):
        cols = list(eval(self.list2.listvar.get()))
        ref = self.ref.var.get()
        if ref:
            cols.append(ref)
        df = self.df.ix[self.startday.get():self.endday.get(), cols]
        return df
            
    def showfigs(self, ptype='line'):
        df = self.get_plot_df()        
        kwargs = {'grid':True, }
        if ptype in ['line', 'bar', 'area']:
            kwargs.update({'xticks':np.arange(0, len(df), (len(df)/60)+1)})
        kwargs.update( _KWARGS.get(ptype, {}) )
        kwargs.update(eval(self.kwargs.get('1.0','1.end'))) # update user kwargs
        try:
            df.plot(**kwargs)          
        except Exception as e:
            self.txt1.insert('end', '{}\n'.format(e))  
        plt.show() 

    def set_enddays(self, event):
        try:
            days = self.startday.cget('values')
            cid = self.startday.current()
            self.endday.config(values=days[cid:])
            self.endday.var.set(days[-1])             
        except Exception as e:
            print e

     
    def run(self):
        self.mainloop()           
    
    def stop(self, event):
        print 'Event:', event.keycode
        self.destroy()

            
if __name__ == '__main__':
    gui = Gui('gui.ini') 
    gui.run()
