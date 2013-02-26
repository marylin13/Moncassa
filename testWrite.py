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
    
    def test_write(self):
        for i in range (0,7200):
            MonCassa.write("metric", long(time.time()), i)
            time.sleep(10)
       

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()