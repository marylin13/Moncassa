#!/usr/local/bin/python
import rpyc
import MonCassa
class MyService(rpyc.Service):
    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        pass

    def exposed_write(self, metric, timestamp, value, tags): # this is an exposed method
        MonCassa.write(metric, long(timestamp), float(value), tags)
        return 'success'
    
    def exposed_read(self, metric, starttime, endtime,tags): # this is an exposed method
        result_points = MonCassa.read(metric, long(starttime), long(endtime), tags)
        return result_points
     
     
#    def exposed_read_all(self, metric, column_family): # this is an exposed method
#         result_points = MonCassa.read_all(metric, column_family)
#         return result_points
   
if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = 18861)
    print 'start rpc server'
    t.start()