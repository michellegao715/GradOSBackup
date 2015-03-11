import sys

import zerorpc
import gevent
import socket
import job 
import mapreduce 

# master_addr='tcp://10.0.0.25:4242'
master_addr='tcp://127.0.0.1:4242'

class Worker(object):
    def __init__(self):
        gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print('[Worker] Ping from Master')

    def do_work(self, mapreduce_class, sub):
        if mapreduce_class == 'wordcount':
          mapper = job.WordCountMap 
          reducer = job.WordCountReduce
        elif mapreduce_class == 'hammingencode':
          mapper = job.HammingEncodeMap
          reducer = job.HammingEncodeReduce
        engine = mapreduce.Engine(sub, mapper, reducer)
        engine.execute()
        result_list = engine.get_result_list()
        
        gevent.sleep(2)
        return result_list  

if __name__ == '__main__':
  s = zerorpc.Server(Worker())
  #master_ip = ip_port.split(':')[0]
  #master_port = ip_port.split(':')[1]
  #ip = '10.0.0.10'
  ip = socket.gethostbyname(socket.gethostname())
  port = '4243'
  s.bind('tcp://'+ip+':'+port)
  c = zerorpc.Client()
  c.connect(master_addr)
  c.register(ip,port)
  s.run()

