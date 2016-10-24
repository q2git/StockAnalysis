# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 10:56:55 2016
@author: q2git
"""

from tkfactory import TkFactory
import statfuncs as s
import plottings as p
import threading
import Queue

            
class Gui(TkFactory):
    def __init__(self, filename, master=None):
        TkFactory.__init__(self, filename, master)
        self.que = Queue.Queue()
        #bind_all
        self.bind_all('<Control-KeyPress-q>', self.stop)
        self.bt_loadstat.config(command= self.loadstat) #  
        self.ktype.bind("<<ComboboxSelected>>", self.set_mas) 
        
    def config_cmd(self):     
        self.bt_s2ref.config(command= self.add2ref)  
        self.bt_show.config(command= lambda: self.showfig(self.ptype.var.get()))
        self.bt_stat.config(command= lambda: self.stat_data(True)) 
        self.bt_excel.config(command= lambda: self.df.to_excel(self.fn.var.get()))
        self.list1.bind('<Double-1>', self.add2plot)
        self.list1.bind('<3>', self.send2plot) 
        self.list2.bind('<Double-1>', 
                        lambda _:self.list2.delete(self.list2.curselection()[0]))
        self.group1.bind("<<ComboboxSelected>>", self.setlist1)        
        self.startday.bind("<<ComboboxSelected>>", self.set_enddays)       
        self.bt_show1.config(command= lambda: self.showfig('bar'))
        self.bt_area.config(command= lambda: self.showfig('area'))
       
    def check_que(self):
        self.txt1.see('end')         
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
        
    def loadstat(self):
        self.bt_loadstat.var.set('Loading...')
        try:
            years = self.years.var.get()
            ktype = self.ktype.var.get()
            mas = self.mas.get().split(',')
            #rmxx = self.rmxx.get().split(',')
            #self.df = stat(years, ktype, window, mas, rmxx)
            threading.Thread(target=s.df_idxs_codes, args=(years, ktype, 
                                mas, self.que)).start()
            self.after(1000, self.check_que)
        except Exception as e:
            print e

    def stat_data(self, re_load=False):
        if re_load: 
            reload(s)
        f = self.func.get()
        w = int(self.window.get())
        k = self.group.get()                
        threading.Thread(target=s.stat, 
                         args=(self.dfi, self.dfc, k, f, w, self.que)).start() # stat thread        
        self.after(1000, self.check_que) 
         
    def setbydf(self):
        if not self.startday.get(): 
            self.config_cmd() # bind all functions
        self.bt_loadstat.var.set('Load & Stat')        
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
        
    def add2ref(self, event=None):
        idx = self.list1.curselection()[0]
        val = self.list1.get(idx)
        self.ref.var.set(val)

    def get_df4plot(self):
        cols = list(eval(self.list2.listvar.get()))
        ref = self.ref.var.get()
        if ref:
            cols.append(ref)
        df = self.df.ix[self.startday.get():self.endday.get(), cols]
        return df
            
    def showfig(self, ptype='line'):
        reload(p)
        df = self.get_df4plot()        
        params = eval(self.params.var.get())         
        msg = p.showfig(df, ptype, params)
        if msg.startswith('plot'):          
            self.txt1.insert('end', msg)
        else:
            self.kwargs.delete('1.0', 'end')
            self.kwargs.insert('end', msg)

    def set_enddays(self, event):
        try:
            days = self.startday.cget('values')
            cid = self.startday.current()
            self.endday.config(values=days[cid:])
            self.endday.var.set(days[-1])             
        except Exception as e:
            print e

    def set_mas(self, event):
        ktype = self.ktype.var.get()
        if ktype=='QFQ':
            self.mas.var.set('5,10,20,30,60')
        else:
            self.mas.var.set('30,60')
     
    def run(self):
        self.mainloop()           
    
    def stop(self, event):
        print 'Event:', event.keycode
        self.destroy()

            
if __name__ == '__main__':
    gui = Gui('gui.ini') 
    gui.run()
