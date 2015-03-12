import sys

import zerorpc
import gevent
import socket
import job
import mapreduce 

# master_addr='tcp://10.0.0.25:4242'
master_addr='tcp://127.0.0.1:4242'

class Worker(object):
    def __init__(self, chunk, mapreduce_method):
        gevent.spawn(self.controller)
        self.mapreduce_method = mapreduce_method
        self.chunk = chunk 
        self.result_list = None  

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print('[Worker] Ping from Master')

    def do_work(self, mapreduce_method, chunk):
        if mapreduce_method == 'wordcount':
          mapper = job.WordCountMap()
          reducer = job.WordCountReduce()
          # wordcount = job.WordCount(chunk,mapper,reducer)
        elif mapreduce_method == 'hammingencode':
          pass
        elif mapreduce_method == 'hammingdecode':
          pass
        elif mapreduce_method == 'hammingerror':
          pass
        elif mapreduce_method == 'hammingcheck':
          pass 
        elif mapreduce_method == 'hammingfix':
          pass
       # map phase
        for i,v in enumerate(chunk):
          mapper.map(i,v)
        table = mapper.get_table()
        keys = table.keys()
       # sort intermediate keys 
        keys.sort()
       # reduce phase 
        for k in keys:
          reducer.reduce(k,table[k])
        print 'print out map reduce results......................'
        #self.result_list = job.WordCount.get_result_list(wordcount)
        self.result_list = reducer.get_result_list()
        print self.result_list


    
chunk = ['cba','ab','abc','ab','abc']
method = 'wordcount'
w  = Worker(chunk, method)
w.do_work(method,chunk)
  
    #ip = socket.gethostbyname(socket.gethostname()) 
     # port = '4243'
      #s.bind('tcp://'+ip+':'+port)
      #c = zerorpc.Client()
      #c.connect(master_addr)
      #c.register(ip,port)
     # s.run() 

