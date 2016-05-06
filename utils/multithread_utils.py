# -*- coding: utf-8 -*-
from __future__ import division
import threading
import multiprocessing
import time
import pandas as pd
import numpy as np
import os

"""
The MultiProcessingDataFrame2 class is recommended to replace MultiProcessingDataFrame class
The MultiThreadProcess class is for multiprocessing on parrallelizing on multiple threads on one processor
The MultiProcessing  class is for parrallelizing on multiple processors, thus recommended.  
"""

class MultiThreadProcess():
    def __init__(self, thread_nb, object_list, worker, **kwarg):
        """
        Params:
        -------
        worker: func
            thread worker, must take a list as input
        object_list: list
            list of object allowing to be distributed to workers
        thread_nb: int
            nb de thread
        """
        self.thread_nb = thread_nb
        self.object_list = object_list
        self.worker = worker
        self.kwarg = kwarg
    
    def __split__(self):
        """split object_list into thread_nb partitions
        """
        size = len(self.object_list)/self.thread_nb
        if int(size) != size: 
            size = int(size) + 1
        size = int(size)
        return [self.object_list[i*size:(i+1)*size] for i in range(self.thread_nb)]   
        
    def run(self):
        splitted = self.__split__()
        for thread_id, thread_list in zip(range(self.thread_nb), splitted):
            if self.kwarg:
                args = (thread_list, self.kwarg,)
            else:
                args = (thread_list,)
            t = threading.Thread(target = self.worker, args = args)
            t.start()
            
               
class MultiProcessingDataFrame():
    """run multiprocessing on dataframes
    """
    def __init__(self, thread_nb, object_df, worker):
        self.thread_nb = thread_nb
        self.object_values = object_df.values
        self.object_header = object_df.columns
        self.worker = worker
    
    def __split__(self):
        """split the dataframe
        """
        size = self.object_values.shape[0]/self.thread_nb
        if int(size) != size: 
            size = int(size) + 1
        size = int(size)
        return [self.object_values[i*size:(i+1)*size, :] for i in range(self.thread_nb)]  
    def run(self):
        splitted = self.__split__() 
        for thread_id, thread_list in zip(range(self.thread_nb), splitted):
            t = multiprocessing.Process(target = self.worker, args = (thread_list,))
            t.start()
            
class MultiProcessingDataFrame2():
    """An upgrade from the previous MultiProcessingDataFrame class
    by adding the header conservation
    
    Each worker takes a dataframe with header as input
    """
    def __init__(self, thread_nb, object_df, worker):
        self.thread_nb = thread_nb
        self.object_values = object_df.values
        self.object_header = object_df.columns
        self.worker = worker
    
    def __split__(self):
        """split the dataframe
        """
        size = self.object_values.shape[0]/self.thread_nb
        if int(size) != size: 
            size = int(size) + 1
        size = int(size)
        return [pd.DataFrame(self.object_values[i*size:(i+1)*size, :], columns = self.object_header) for i in range(self.thread_nb)]  
    def run(self):
        splitted = self.__split__() 
        for thread_id, thread_list in zip(range(self.thread_nb), splitted):
            t = multiprocessing.Process(target = self.worker, args = (thread_list,))
            t.start()
                      
class MultiProcessing():
    def __init__(self, thread_nb, object_list, worker, **kwarg):
        """
        Params:
        -------
        worker: func
            thread worker, must take a list as input
        object_list: list
            list of object allowing to be distributed to workers
        thread_nb: int
            nb de thread
        """
        self.thread_nb = thread_nb
        self.object_list = object_list
        self.worker = worker
        self.kwarg = kwarg
        
    
    def __split__(self):
        """split object_list into thread_nb partitions
        """
        size = len(self.object_list)/self.thread_nb
        if int(size) != size: 
            size = int(size) + 1
        size = int(size)
        return [self.object_list[i*size:(i+1)*size] for i in range(self.thread_nb)]   
        
    def run(self):
        splitted = self.__split__() 
        for thread_id, thread_list in zip(range(self.thread_nb), splitted):
            if self.kwarg:
                args = (thread_list, self.kwarg,)
            else:
                args = (thread_list,)
            t = multiprocessing.Process(target = self.worker, args = args)
            t.start()
class DynMultiProcessing():
    """dynamic multiprocessing
    """
    def __init__(self, thread_nb, object_list, worker, chunk_size, **kwarg):
        """
        Params:
        -------
        worker: func
            thread worker, must take a list as input
        object_list: list
            list of object allowing to be distributed to workers
        thread_nb: int
            nb de thread
        """
        self.thread_nb = thread_nb
        self.object_list = object_list
        self.worker = worker
        self.chunk_size = chunk_size
        self.kwarg = kwarg
        
        self.counter = 0 
        self.running_thread = []
    
    def __retrieve__(self):
        res = self.object_list[self.counter*self.chunk_size:(self.counter+1)*self.chunk_size]
        
        self.counter += 1
        return res
    
    def __split__(self):
        size = int(self.chunk_size)
        nb = len(self.object_list)/float(size)
        if nb != int(nb):
            nb = int(nb) + 1
        else:
            nb = int(nb)
        return [self.object_list[i*size:(i+1)*size] for i in range(nb)]
   
    def __run_new_process__(self):
        thread_list = self.__retrieve__()
        if len(thread_list) == 0:
            return None
        else:
            if self.kwarg:
                args = (thread_list, self.kwarg,)
            else:
                args = (thread_list,)
            t = multiprocessing.Process(target = self.worker, args = args)
            print(multiprocessing.current_process(), 'Starting')
            t.start()
            self.running_thread.append(t)
            
    def run_pool(self):
        splitted = self.__split__()
        pool = multiprocessing.Pool(processes=self.thread_nb)
        for x in splitted:
            if self.kwarg:
                pool.apply_async(self.worker, (x, self.kwarg,))
            else:
                pool.apply_async(self.worker, (x,))
        pool.close()
        pool.join()                
    
def simple_pool_worker(*arg):
    if len(arg) == 1:
        print(sum(arg[0]))
    elif len(arg) == 2:
        for k, v in arg[1].items():
            print(v)
    else:
        print("error")
    
    
    # mysum = sum(mylist)
    # DIR = os.path.dirname(__file__)
    # os.makedirs(os.path.join(DIR, "%d"%mysum))
    """
    print(multiprocessing.current_process(), 'Starting')
    time.sleep(1)
    print(sum(mylist))
    print(multiprocessing.current_process(), 'Exiting') 
    """
    

def simple_worker(*arg):
    print(multiprocessing.current_process(), 'Starting')
    # First get worker arguments
    # *******************************
    value_list= arg[0]
    if len(arg) > 0:
        arg1, arg2 = arg[1].values()
    # *******************************
    time.sleep(2)
    for value in value_list:
        print(arg1/arg2)
    print(multiprocessing.current_process(), 'Exiting')
def simple_worker2(value_list):
    # previous codes still work
    print(multiprocessing.current_process(), 'Starting')
    time.sleep(2)
    for value in value_list:
        print(value)
    print(multiprocessing.current_process(), 'Exiting')
def simple_worker_df(df):
    print(multiprocessing.current_process(), 'Starting')
    time.sleep(2)
    print(df.columns)
    print(multiprocessing.current_process(), 'Exiting')

def main_simple_pool_worker():
    th = DynMultiProcessing(thread_nb = 3, object_list = list(range(100)),\
                             worker = simple_pool_worker, chunk_size = 10, arg1 = 1)  
    th.run_pool()
    
    
    
def main_simple_worker():
    value_list = range(12)
    th = MultiProcessing(thread_nb = 4, object_list = value_list, worker = simple_worker, arg1 = 1, arg2 = 2)
    th.run()
def main_simple_worker2():
    value_list = range(12)
    th = MultiProcessing(thread_nb = 4, object_list = value_list, worker = simple_worker2)
    th.run()
def main_simple_worker_df():
    df = pd.DataFrame(np.random.rand(20, 4), columns = ["test%d"%x for x in range(1, 5)])
    th = MultiProcessingDataFrame2(thread_nb = 4, object_df = df, worker = simple_worker_df)
    th.run()
    
    

    
"""            
def extraction_worker_addToBasket(date_list):
    print threading.currentThread().getName(), 'Starting'
    for day in date_list:
        extract.retrieve_addToBasket_by_day(day)
    print threading.currentThread().getName(), 'Exiting'

def main_make_reduced_files(begin, num):
    date_list = utility.make_datelist(begin, num)
    th = MultiProcessing(thread_nb = 20, object_list = date_list, worker = make_reduced_file_worker)
    th.run()
"""    
    
if __name__ == "__main__":
    main_simple_pool_worker()
    # dmp = DynMultiProcessing(thread_nb = 20, object_list = list(range(10)), worker = sum, chunk_size = 3)
    # dmp.test()
    # main_simple_worker_df()
    # main_simple_worker()
    
