#!/usr/local/bin/python
# -*- coding: utf-8 -*-

from collections import defaultdict
import pycassa
from pycassa.pool import ConnectionPool
import datetime
import time
import sys
import struct

dictAvg60 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
dictAvg300 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
dictAvg7200 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
dictAvg86400 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
address = 'localhost:9160'
keyspace = 'monitor'
upertime_interval = 2592000

#init maxID in database
def init():
    init_id = '\x00\x00'
    pool = ConnectionPool(keyspace, [address])
    col_fam_moncassa_meta_id = pycassa.ColumnFamily(pool, 'moncassa_meta_id') 
    col_fam_moncassa_meta_id.insert('maxID', {'metric': init_id, 'tagk': init_id, 'tagv':init_id})
    

def write(metric , timestamp, value, tags):
    pool = ConnectionPool(keyspace, [address])
    upertime = timestamp/upertime_interval
#    get key from database, if some id is not exist, create new one
    key = generate_key(metric, upertime, tags) 
    
#    save to rawdata
    pool = ConnectionPool(keyspace, [address])
    col_fam_rawdata = pycassa.ColumnFamily(pool, 'rawdata') 
    col_fam_rawdata.insert(key, {timestamp: value})  
    
#   save to rollups60，if in the same minute , update the memory. 
#   if it is new minute, write the old value to cassandra, update the memory
    if dictAvg60[metric]['timestamp'] == 0:
        dictAvg60[metric]['avg'] = value
        dictAvg60[metric]['counter'] = 1
    elif inOneMinute(timestamp, dictAvg60[metric]['timestamp']):
        newAvg  = caculate(dictAvg60[metric]['avg'], dictAvg60[metric]['counter'], value)
        dictAvg60[metric]['avg'] = newAvg
        dictAvg60[metric]['counter'] += 1
    else:
        col_fam_rollups60 = pycassa.ColumnFamily(pool, 'rollups60')
        col_fam_rollups60.insert(metric, {dictAvg60[metric]['timestamp']:  dictAvg60[key]['avg']})  
        dictAvg60[metric]['avg'] = value
        dictAvg60[metric]['counter'] = 1
    dictAvg60[metric]['timestamp'] = timestamp
    
 #   save to rollups300
    if dictAvg300[metric]['timestamp'] == 0:
        dictAvg300[metric]['avg'] = value
        dictAvg300[metric]['counter'] = 1
    elif inFiveMinutes(timestamp, dictAvg300[metric]['timestamp']):
        newAvg  = caculate(dictAvg300[metric]['avg'], dictAvg300[metric]['counter'], value)
        dictAvg300[metric]['avg'] = newAvg
        dictAvg300[metric]['counter'] += 1
    else:
        col_fam_rollups300 = pycassa.ColumnFamily(pool, 'rollups300')
        col_fam_rollups300.insert(metric, {dictAvg300[metric]['timestamp']:  dictAvg300[key]['avg']})  
        dictAvg300[metric]['avg'] = value
        dictAvg300[metric]['counter'] = 1
    dictAvg300[metric]['timestamp'] = timestamp
    
#   save to rollups7200
    if dictAvg7200[metric]['timestamp'] == 0:
        dictAvg7200[metric]['avg'] = value
        dictAvg7200[metric]['counter'] = 1
    elif inTwoHours(timestamp, dictAvg7200[metric]['timestamp']):
        newAvg  = caculate(dictAvg7200[metric]['avg'], dictAvg7200[metric]['counter'], value)
        dictAvg7200[metric]['avg'] = newAvg
        dictAvg7200[metric]['counter'] += 1
    else:
        col_fam_rollups7200 = pycassa.ColumnFamily(pool, 'rollups7200')
        col_fam_rollups7200.insert(metric, {dictAvg7200[metric]['timestamp']:  dictAvg7200[key]['avg']})  
        dictAvg7200[metric]['avg'] = value
        dictAvg7200[metric]['counter'] = 1
    dictAvg7200[metric]['timestamp'] = timestamp
    
#   save to rollups86400
    if dictAvg86400[metric]['timestamp'] == 0:
        dictAvg86400[metric]['avg'] = value
        dictAvg86400[metric]['counter'] = 1
    elif inOneDay(timestamp, dictAvg86400[metric]['timestamp']):
        newAvg  = caculate(dictAvg86400[metric]['avg'], dictAvg86400[metric]['counter'], value)
        dictAvg86400[metric]['avg'] = newAvg
        dictAvg86400[metric]['counter'] += 1
    else:
        col_fam_rollups86400 = pycassa.ColumnFamily(pool, 'rollups86400')
        col_fam_rollups86400.insert(metric, {dictAvg86400[metric]['timestamp']:  dictAvg86400[key]['avg']})  
        dictAvg86400[metric]['avg'] = value
        dictAvg86400[metric]['counter'] = 1
    dictAvg86400[metric]['timestamp'] = timestamp
    pool.dispose();
    
#    if no point between start time and end time, return {}
#    if no metric , return None
def read(metric, start_time, end_time, tags):
    pool = ConnectionPool(keyspace, [address])
#    decide which column family to read based on time diffrence
    if timeDiff(start_time, end_time) <= 3600:
        col_fam = pycassa.ColumnFamily(pool, 'rawdata')
    elif timeDiff(start_time, end_time) <= 7200:
        col_fam = pycassa.ColumnFamily(pool, 'rollups60')
    elif timeDiff(start_time, end_time) <= 86400:
        col_fam = pycassa.ColumnFamily(pool, 'rollups300')
    elif timeDiff(start_time, end_time) <= 2592000:
        col_fam = pycassa.ColumnFamily(pool, 'rollups7200')
    else:
        col_fam = pycassa.ColumnFamily(pool, 'rollups86400') 
        
#  change start_time , end_time to uper timestamp
    start_upertime = start_time/upertime_interval
    end_updertime = end_time/upertime_interval
    points = {}
    for i in range(start_upertime, end_updertime + 1):
        key = generate_key(metric, i, tags)
        try:
            points = col_fam.get(key, column_start=start_time, column_finish=end_time)
        except pycassa.NotFoundException:
            return None
    pool.dispose()
    return points
    
    
def read_keys(column_family):
    pool = ConnectionPool(keyspace, [address])
    col_fam = pycassa.ColumnFamily(pool, column_family) 
     # Since get_range() returns a generator - print only the keys.
    for value in col_fam.get_range(column_count=0,filter_empty=False):
        print value[0]

def read_keys_and_column(column_family):
    pool = ConnectionPool(keyspace, [address])
    col_fam = pycassa.ColumnFamily(pool, column_family) 
    for value in col_fam.get_range(column_count=0,filter_empty=False):
        print value[0] 
        print str( col_fam.get( value[0] ) )
        print ""


def generate_key(metric,upertime, tags):
    key = metric +'|' + str(upertime) 
    for k in tags:
        tag_item = '|' + k + '=' + tags[k]  
        key += tag_item
    return key
        

def caculate(oldAvg, counter, value):
    return oldAvg + (float(value) - float(oldAvg))/(counter+1)

def timeDiff(time1, time2):
    return time2-time1;
    
def inOneMinute(time1, time2):
    return convert2min(time1) == convert2min(time2)

def inFiveMinutes(time1, time2):
    return  convert2hour(time1) == convert2hour(time2) and time.gmtime(time1).tm_min/5 == time.gmtime(time2).tm_min/5

def inTwoHours(time1, time2):
    return convert2day(time1) == convert2day(time2) and  time.gmtime(time1).tm_hour/2 ==  time.gmtime(time2).tm_hour/2

def inOneDay(time1, time2):
    return convert2day(time1) == convert2day(time2)

def convert2month(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m')

def convert2day(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    
def convert2hour(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H')
      
def convert2min(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M') 

def convert2sec(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')