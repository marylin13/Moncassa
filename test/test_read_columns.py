#!/usr/local/bin/python
# -*- coding: utf-8 -*-
'''
Created on Feb 26, 2013

@author: ld
'''

import unittest
import MonCassa
import time


class Test(unittest.TestCase):
    def test_read_keys(self):
        MonCassa.read_keys("rawdata")
        
#    def test_read_keys_and_column(self):
#        MonCassa.read_keys_and_column("metric_meta")
        

#   test read cpu
    def test_read1(self):
        metric = 'cpu.cpu'
        tags = {'host':'ld-VirtualBox', 'type_instance':'nice','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'nice'
        print result_points
    
    def test_read2(self):
        metric = 'cpu.cpu'
        tags = {'host':'ld-VirtualBox', 'type_instance':'idle','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'idle'
        print result_points
        
    def test_read3(self):
        metric = 'cpu.cpu'  
        tags = {'host':'ld-VirtualBox', 'type_instance':'softirq','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'softirq'
        print result_points
        
    def test_read4(self):
        metric = 'cpu.cpu'  
        tags = {'host':'ld-VirtualBox', 'type_instance':'wait','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'wait'
        print result_points
    
    def test_read5(self):
        metric = 'cpu.cpu'  
        tags = {'host':'ld-VirtualBox', 'type_instance':'system','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'system'
        print result_points
        
    
    def test_read6(self):
        metric = 'cpu.cpu'  
        tags = {'host':'ld-VirtualBox', 'type_instance':'user','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'user'
        print result_points
    
    def test_read7(self):
        metric = 'cpu.cpu'  
        tags = {'host':'ld-VirtualBox', 'type_instance':'interrupt','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'interrupt'
        print result_points
        
    def test_read8(self):
        metric = 'cpu.cpu'  
        tags = {'host':'ld-VirtualBox', 'type_instance':'steal','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print 'steal'
        print result_points
        

 
    
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()