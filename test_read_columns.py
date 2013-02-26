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
    
    def test_read_all(self):
        dict_raw_data = MonCassa.read_all('metric','rawdata')
        dict_rollups60 = MonCassa.read_all('metric','rollups60')
        dict_rollups300 = MonCassa.read_all('metric','rollups300')
        for key in dict_raw_data:
            print MonCassa.convert2sec(key), ': ', dict_raw_data[key]
        print ''
        for key in dict_rollups60:
            print MonCassa.convert2min(key), ': ', dict_rollups60[key]
        print ''
        for key in dict_rollups300:
            print MonCassa.convert2min(key), ': ', dict_rollups300[key]
       

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()