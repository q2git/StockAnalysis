# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 10:56:55 2016

@author: q2git
"""

from tkfactory import TkFactory, add_scrollbar
from statfuncs import stat
import numpy as np
import matplotlib.pyplot as plt


class Gui(TkFactory):
    def __init__(self, filename, master=None):
        TkFactory.__init__(self, filename, master)
        self.config_cmd()

        
    def config_cmd(self):
        #bind_all
        self.bind_all('<Control-KeyPress-q>', self.stop)
        self.bt1.config(command= self.load_stat)        
        self.bt2.config(command= self.send_plot)  
        self.bt3.config(command= self.show_figs)
        self.list1.bind('<Double-1>', self.set_ref)

    def set_ref(self, event):
        idx = self.list1.curselection()[0]
        val = self.list1.get(idx)
        self.ref.var.set(val)
        
    def load_stat(self):
        gui.txt1.insert('1.0', 'Starting\n') 
        self.df = stat()
        self.list1.listvar.set(tuple(self.df.columns.sort_values().tolist()))        
        gui.txt1.insert('1.0', 'Done.\n') 
        
    def send_plot(self):
        gui.txt1.insert('1.0', 'Starting\n')
        idxs = self.list1.curselection()
        vals = tuple(map(self.list1.get, idxs))
        self.list2.listvar.set(vals)
        
    def show_figs(self):
        cols = list(eval(self.list2.listvar.get()))
        ref = self.ref.var.get()
        if ref:
            cols.append(ref)
        df = self.df[cols]
        df.plot(subplots=True, marker='.', x_compat=True,
                xticks=np.arange(0, len(df), (len(df)/60)+1), )
        plt.show() 
        
    def run(self):
        self.mainloop()           
    
    def stop(self, event):
        print 'Event:', event.keycode
        self.destroy()

            
if __name__ == '__main__':
    gui = Gui('gui.ini')
    add_scrollbar(gui.txt1, hsb=False)
    gui.run()
