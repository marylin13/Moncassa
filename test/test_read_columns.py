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
#    def test_read_keys(self):
#        MonCassa.read_keys("rawdata")
        
#    def test_read_keys_and_column(self):
#        MonCassa.read_keys_and_column("metric_meta")
        

#   test read cpu
    def test_read1(self):
        metric = 'cpu.cpu'
        tags = {'host':'ld-VirtualBox', 'type_instance':'nice','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print result_points
        
    def test_read2(self):
        metric = 'cpu.cpu'
        tags = {'host':'ld-VirtualBox', 'type_instance':'idle','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        result_points = MonCassa.read(metric,  start_time, end_time, tags)
        print result_points
    
    def test_read3(self):
        metric = 'metric'
        start_time = 1361859418
        end_time = 1361860214
        result_points = MonCassa.read(metric, long(start_time), long(end_time), {'host':'ji'})
        print result_points
        
    
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()