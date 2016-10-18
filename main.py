# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 10:56:55 2016

@author: q2git
"""

from tkfactory import TkFactory, add_scrollbar
from statfuncs import stat
import numpy as np
import matplotlib.pyplot as plt
import threading
import Queue


class Gui(TkFactory):
    def __init__(self, filename, master=None):
        TkFactory.__init__(self, filename, master)
        self.que = Queue.Queue()
        self.config_cmd()
        
    def config_cmd(self):
        #bind_all
        self.bind_all('<Control-KeyPress-q>', self.stop)
        self.bt1.config(command= self.load_stat)        
        self.bt2.config(command= self.send_plot)  
        self.bt3.config(command= self.show_figs)
        self.list1.bind('<Double-1>', self.set_ref)
        self.startday.bind("<<ComboboxSelected>>", self.set_enddays)

    def check_que(self):
        try:
            flag, data = self.que.get_nowait()
            if flag:
                self.df = data
                self.setbydf()
            else:
                self.txt1.insert('end', data)
                self.after(1000, self.check_que)                
        except Queue.Empty:
            self.txt1.insert('end', '.') 
            self.after(1000, self.check_que)
        #else:
            #self.after(1000, self.check_que)
        
    def set_ref(self, event):
        idx = self.list1.curselection()[0]
        val = self.list1.get(idx)
        self.ref.var.set(val)
        
    def load_stat(self):
        self.bt1.var.set('Loading...')
        try:
            years = self.years.var.get()
            ktype = self.ktype.var.get()
            mas = map(lambda x: int(x), self.mas.get().split(','))
            rmxx = map(lambda x: int(x), self.rmxx.get().split(','))
            window = int(self.window.get())
            #self.df = stat(years, ktype, window, mas, rmxx)
            threading.Thread(target=stat, args=(years, ktype, window, 
                                mas, rmxx, self.que)).start()
            self.check_que()
        except Exception as e:
            print e

    def setbydf(self):
        self.bt1.var.set('Load & Stat & Plot')        
        days = self.df.index.tolist() 
        self.startday.config(values=days)
        self.startday.var.set(days[0])
        self.endday.var.set(days[-1])             
        self.list1.listvar.set(tuple(self.df.columns.sort_values().tolist()))                     
        
    def send_plot(self):
        idxs = self.list1.curselection()
        vals = tuple(map(self.list1.get, idxs))
        self.list2.listvar.set(vals)
        
    def show_figs(self):
        cols = list(eval(self.list2.listvar.get()))
        ref = self.ref.var.get()
        if ref:
            cols.append(ref)
        df = self.df.ix[self.startday.get():self.endday.get(), cols]
        df.plot(subplots=True, marker='.', x_compat=True,
                xticks=np.arange(0, len(df), (len(df)/60)+1), )
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
    gui = Gui('gui.txt')
    add_scrollbar(gui.txt1, hsb=False)
    gui.run()
