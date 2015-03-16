import sys

import zerorpc
import gevent
import socket

master_addr='tcp://127.0.0.1:4242'
# python mr_worker.py <ip_address_master:port>
class Worker(object):
  def __init__(self, chunk_data, mapreduce_method):
        gevent.spawn(self.controller)
        self.mapreduce_method = mapreduce_method
        self.chunk_data= chunk_data
        self.result_list = None  

  def controller(self):
    while True:
      print('[Worker]')
      gevent.sleep(1)

  def ping(self):
    print('[Worker] Ping from Master') 
# chunk_data: <offset,length> 

  # TODO do map and write intermediate file into different outputs for reducers  
  def map(self, method_class, chunk_data, num_reducers):
    print 'in map() function in worker' 
    pass
  # TODO ask every worker in ips_mapper for input_file and then reduce 
  def reduce(self, ips_mapper, input_file):
    pass

if __name__ == '__main__':
  ip = socket.gethostbyname(socket.gethostname())
  port = '4243'
  #TODO pass method and chunkdata for testing
  chunkdata = [1,2,3]
  method = 'wordcount'
  s = zerorpc.Server(Worker(chunkdata,method))
  s.bind('tcp://'+ip+':'+port)
  c = zerorpc.Client()
  c.connect(master_addr)
  c.register(ip,port)
  s.run()

