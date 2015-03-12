import sys

import zerorpc
import gevent
import socket
import job
import mapreduce 

# master_addr='tcp://10.0.0.25:4242'
master_addr='tcp://127.0.0.1:4242'
input_file = 'test' 

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

    # get content from chunk_datar: <offset,length> 
    def fetch_chunk(self, mapreduce_method, chunk_data):
      if mapreduce_method == 'wordcount':
        #TODO read by bytes and then combine into words(seperated by /n)  
      else:
        in_file = open(input_file,'rb')
        offset = chunk_data[0]
        length = chunk_data[1]
        start= in_file.read(offset)
        data = in_file.read(length)
        return data
        
    def do_work(self, mapreduce_method, chunk_data):
        if mapreduce_method == 'wordcount':
          mapper = job.WordCountMap()
          reducer = job.WordCountReduce()
          # wordcount = job.WordCount(chunk,mapper,reducer)
        elif mapreduce_method == 'hammingencode':
          mapper = job.EncodeMap()
          reducer = job.EncodeReduce()
        elif mapreduce_method == 'hammingdecode':
          pass
        elif mapreduce_method == 'hammingerror':
          pass
        elif mapreduce_method == 'hammingcheck':
          pass 
        elif mapreduce_method == 'hammingfix':
          pass
       # map phase
        chunk =self.fetch_chunk(mapreduce_method, chunk_data)
        # if wordcount, combine words from bytes 
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
        self.result_list = reducer.get_result_list()
        print self.result_list


# testing for word count 
f = open('test','r')
chunk_data=[0,59]  # chunk_data is list, first element is offset, second element is length 
method = 'wordcount'
w  = Worker(chunk_data, method)
w.do_work(method,chunk_data)
  
    #ip = socket.gethostbyname(socket.gethostname()) 
     # port = '4243'
      #s.bind('tcp://'+ip+':'+port)
      #c = zerorpc.Client()
      #c.connect(master_addr)
      #c.register(ip,port)
     # s.run() 

