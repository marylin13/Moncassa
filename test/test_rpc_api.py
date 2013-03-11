import unittest
import rpyc
import time

class Test(unittest.TestCase):
    
    def test_rpc_api_write(self):
       c = rpyc.connect("localhost", 18861)
       print  c.root.write("cpu9", long(time.time()), 7.0, {'direction':'in'})
       c.close()
       
    def test_rpc_api_read_all(self):
       c = rpyc.connect("localhost", 18861)
       print  c.root.read("cpu9", long(time.time())-308, long(time.time()), {'direction':'in'})
       c.close()
    

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()


