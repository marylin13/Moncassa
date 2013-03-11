import unittest
import requests
import time
import json

class Test(unittest.TestCase):
    
    def test_webapi_write(self):
        tags = json.dumps({'host': 'pc10'})
        monitor_point = {'metric': 'test3', 'timestamp': long(time.time()), 'value': 7 , 'tags': tags}
        resp = requests.post('http://localhost:5000/write', data = monitor_point)
        print resp.content
    
    def test_webapi_read(self):
        metric = 'cpu.cpu'
        tags = {'host':'ld-VirtualBox', 'type_instance':'idle','plugin_instance': '0'}
        start_time = 1362903850
        end_time = 1362903940
        tags_json = json.dumps(tags)
        search_input = {'metric': metric, 'starttime':  start_time, 'endtime':  end_time,'tags': tags_json}
        resp = requests.post('http://localhost:5000/read', data = search_input)
        print resp.content
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()


