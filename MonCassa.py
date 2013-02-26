#!/usr/local/bin/python
# -*- coding: utf-8 -*-

from collections import defaultdict
import pycassa
from pycassa.pool import ConnectionPool
import datetime
import time

dictAvg60 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
dictAvg300 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
dictAvg7200 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
dictAvg86400 = defaultdict(lambda : {'counter': 0, 'avg': 0, 'timestamp': 0})
address = 'localhost:9160'


def write(metric , timestamp, value):
#    save to RawData
    pool = ConnectionPool('Monitor', [address])
    col_fam_rawdata = pycassa.ColumnFamily(pool, 'rawdata') 
    col_fam_rawdata.insert(metric, {timestamp: value})  

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
        col_fam_rollups60.insert(metric, {dictAvg60[metric]['timestamp']:  dictAvg60[metric]['avg']})  
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
        col_fam_rollups300.insert(metric, {dictAvg300[metric]['timestamp']:  dictAvg300[metric]['avg']})  
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
        col_fam_rollups7200.insert(metric, {dictAvg7200[metric]['timestamp']:  dictAvg7200[metric]['avg']})  
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
        col_fam_rollups86400.insert(metric, {dictAvg86400[metric]['timestamp']:  dictAvg86400[metric]['avg']})  
        dictAvg86400[metric]['avg'] = value
        dictAvg86400[metric]['counter'] = 1
    dictAvg86400[metric]['timestamp'] = timestamp
    pool.dispose();
    
    
def read(metric, startTime, endTime):
    pool = ConnectionPool('Monitor', [address])
    
    if timeDiff(startTime, endTime) <= 3600:
        col_fam = pycassa.ColumnFamily(pool, 'rawdata')
    elif timeDiff(startTime, endTime) <= 7200:
        col_fam = pycassa.ColumnFamily(pool, 'rollups60')
    elif timeDiff(startTime, endTime) <= 86400:
        col_fam = pycassa.ColumnFamily(pool, 'rollups300')
    elif timeDiff(startTime, endTime) <= 2592000:
        col_fam = pycassa.ColumnFamily(pool, 'rollups7200')
    else:
        col_fam = pycassa.ColumnFamily(pool, 'rollups86400') 
    dict = col_fam.get(metric, column_start=startTime, column_finish=endTime)
    pool.dispose()
    return dict
    
    
def read_all(metric, column_family):
    pool = ConnectionPool('Monitor', [address])
    col_fam = pycassa.ColumnFamily(pool, column_family) 
    dict = col_fam.get(metric)
    pool.dispose()
    return dict
   


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
