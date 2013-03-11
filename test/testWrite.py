#!/usr/local/bin/python
# -*- coding: utf-8 -*-
'''
Created on Feb 26, 2013

@author: ld
'''

import unittest
import MonCassa
import time
import pycassa
from pycassa.pool import ConnectionPool

class Test(unittest.TestCase):
    
    def test_write(self):
        for i in range (0,7200):
            MonCassa.write('cpu', long(time.time()), 8, {'host':'pc' + str(i) })
            time.sleep(10)
    
    
#    def test_write(self):
#        MonCassa.write('net_byte', long(time.time()), 8, {'host':'home4'})

#    def test_get_or_create_id(self):
#        id = MonCassa.get_or_create_id('test_sam2', 'metric', True)
#        pool = ConnectionPool('monitor', ['localhost:9160'])
#        col_fam_moncassa_meta_id = pycassa.ColumnFamily(pool, 'moncassa_meta_id')
#        id_test = col_fam_moncassa_meta_id.get('test_sam2',columns=['metric'])['metric']
#        self.assertEqual(id_test, id)
#        
#    def test_get_or_create_key(self):
#        metric = 'test_sam2'
#        upertime = 225
#        tags = {'host':'home'} 
#        key = MonCassa.get_or_create_key(metric, upertime, tags, True)
#        print key
#        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()