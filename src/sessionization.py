#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 08:33:53 2018

@author: kyleguan
"""

import sys
import os
from glob import glob
import csv
from datetime import datetime
import time
from heapq import heapify, heappush, heappop
import argparse



def tuple_to_string(tup, key, t_base):
     '''
      A helper funciton that converts dictionary entry to string for output file
     Input:
        tup: dictionary value in tuple
        key: dictionary key
        t_base: starting timestamp as a base
     Return:
        rslt_str: string in the required format
     '''
     start, end = tup[0] + t_base, tup[1] + t_base
     dt, no_docs = tup[1] - tup[0]+1, tup[4]
     start_str = datetime.fromtimestamp(start)
     end_str = datetime.fromtimestamp(end)
     dt_str = str(int(dt))
     no_docs_str = str(no_docs)
     rslt_str = key+','+str(start_str)+','+str(end_str)+','+dt_str+','+no_docs_str
     return rslt_str
     

def get_log_data(csv_fname):
    '''
    A generator for the data in log.csv. Since csv files can contain over 10 
    millions of records, it is not necessary to store all the record in memory
    all at once.

    Input: csv_fname:
        filename/location of the csv.
    Output: log_record    
    ''' 
    with open(csv_fname, "r") as log_records:
        for log_record in csv.reader(log_records, delimiter=','):
          yield log_record

def check_data(row):
    '''
    Helper function to check if each record has the missing information or not
    Input: row from readint the csv
    Output: True or False
    '''
    if len(row[0])>0 and len(row[1])>0 and len(row[2])>0 and len(row[4])>0 and len(row[5])>0 and len(row[6])>0:
          return True
    else:
          return False
    
    
class priority_dict(dict):
    '''
    Dictionary that can be used as a priority queue.
    Keys of the dictionary are IP addresses, and values
    are a tuple of timestamps to be used as priorities. 
    this class is used for removing expired sessions.
    Adpated from: https://gist.github.com/matteodellamico/4451520
    The advantage over a standard heapq-based priority queue is
    that priorities of items can be efficiently updated (amortized O(1))
    using code as 'thedict[item] = new_priority.'
    The 'smallest' method can be used to return the object with lowest
    priority, and 'pop_smallest' also removes it.
    '''
    
    def __init__(self, *args, **kwargs):
        super(priority_dict, self).__init__(*args, **kwargs)
        self._rebuild_heap()

    def _rebuild_heap(self):
        self._heap = [(v, k) for k, v in self.items()]
        heapify(self._heap)

    def smallest(self):
        '''
        This method returns the item with the lowest priority.
        IndexError will be raised if the object is empty.
        '''
        heap = self._heap
        v, k = heap[0]
        while k not in self or self[k] != v:
            heappop(heap)
            v, k = heap[0]
        return k

    def pop_smallest(self):
        '''
        This method returns the item with the lowest priority and removes it.
        IndexError  will be raised if the object is empty.
        '''  
        heap = self._heap
        v, k = heappop(heap)
        while k not in self or self[k] != v:
            v, k = heappop(heap)
        del self[k]
        return k

    def __setitem__(self, key, val):
        # We are not going to remove the previous value from the heap,
        # since this would have a cost O(n).
        
        super(priority_dict, self).__setitem__(key, val)
        
        if len(self._heap) < 2 * len(self):
            heappush(self._heap, (val, key))
        else:
            # When the heap grows larger than 2 * len(self), we rebuild it
            # from scratch to avoid wasting too much memory.
            self._rebuild_heap()

    def setdefault(self, key, val):
        if key not in self:
            self[key] = val
            return val
        return self[key]

    def update(self, *args, **kwargs):
        super(priority_dict, self).update(*args, **kwargs)
        self._rebuild_heap()
 



if __name__ == '__main__':        
    s_time = time.time()
    
    
    # parser setup
    parser = argparse.ArgumentParser()
    parser.add_argument('input_log')
    parser.add_argument('input_inactivity')
    parser.add_argument('output')

    # input parsing
    namespace = parser.parse_args(sys.argv[1:])

    input_file1 = open(namespace.input_inactivity,'r')
    
    INACTIVITY_PERIOD = float(input_file1.read()[0])
    
    # Initialize priority dictionary to keep track of the session expiration 
    # time
    pd=priority_dict() 
    # Initialize the hash table (dictionary) to keep track active IP addresses
    # and the associated information     
    dict_ip_sess = {}
    
    
    #output_file = open('insight_testsuite/tests/kcg_test11/output/sessionization.txt', 'w')
    output_file = open(namespace.output, 'w')
    time_base = 0  # initialize starting time stamp
    prev = 0   # initialize previous time stamp
    
    # Use generator to get all the rows from 'log.csv'
    rows = iter(get_log_data(namespace.input_log))
    next(rows, None) # skip the header
    for (i, row)  in enumerate(rows):  
        if check_data (row):
            ip = row[0] # IP addres
            # Get the timestamp information
            s_t = row[1] + " " + row[2]
            d = datetime.strptime(s_t, "%Y-%m-%d %H:%M:%S") 
            if i==0:
                time_base = time.mktime(d.timetuple()) 
            # Define session starting time    
            start=time.mktime(d.timetuple()) 
            start -=time_base
            # Define session ending time
            end = start
            # Define session expiration time (if no session in the next time slot)
            exp = start + INACTIVITY_PERIOD+1
            
            
            # If there is a new timestamp
            # Update both dict_ip_sess, and pd, i.e., delete the IP addresses for the
            # detected (expired) session
            
            if (start > prev): 
                 while(len(pd)>0 and pd[pd.smallest()][0]<=start):
                    tmp_ip = pd.smallest()
                    pd.pop_smallest()
                    tmp_tuple = dict_ip_sess[tmp_ip]
                    output_file.write("%s\n" % tuple_to_string(tmp_tuple, tmp_ip, time_base))
                    dict_ip_sess.pop(tmp_ip, None)
            # If this is a new session  
            if ip not in dict_ip_sess:
                doc_counter = 1
                # Use IP address as the key, and the tuple of session starting time
                #, ending time, expiration time, row number, and doc_counter as
                # value
                dict_ip_sess[ip]= (start, end, exp, i, doc_counter)
                # Use IP address as the key, and the tuple of (expiration time, starting time,
                # row number as the tuple.
                pd[ip] = (exp, start, i)
            else: # If this is an on-going session
                # Collect information
                prev_start = dict_ip_sess[ip][0]
                prev_row_num = dict_ip_sess[ip][3]
                prev_exp = dict_ip_sess[ip][2]
                doc_counter = dict_ip_sess[ip][4]
                doc_counter += 1
                # If this is a new time slot
                if (start > prev_start) and (start<=prev_exp):
                    dict_ip_sess[ip]= (prev_start, end, exp, prev_row_num,
                                doc_counter)
                    # New expiration time but keep previous start time and row number
                    pd[ip] = (exp, prev_start, prev_row_num)
                else: 
                    dict_ip_sess[ip]= (start, end, exp, i,
                                doc_counter)
                    pd[ip] = (exp, start, i)
            
            prev = start
        
    # After going through all the rows in log.csv, update all the keys in pd 
    # with new priority tuples
    
    for key in pd:
        tmp_start, tmp_row = pd[key][1], pd[key][2]
        pd[key]=(tmp_start, tmp_row)
    
    # Output the information based on sorting          
    while(len(pd)>0):
        tmp_ip= pd.smallest()
        tmp_tuple = dict_ip_sess[tmp_ip]
        output_file.write("%s\n" % tuple_to_string(tmp_tuple, tmp_ip, time_base))
        pd.pop_smallest()
     
    # close the file    
    output_file.close()    
    e_time = time.time()
    #print('e_time - s_time)
                
                    
                
            
            
           
       
