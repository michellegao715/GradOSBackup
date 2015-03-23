import sys
import socket
import zerorpc
from zerorpc.exceptions import TimeoutExpired
import gevent
import logging


ready  = 'READY'
working  = 'WORKING'
finished    = 'FINISHED'
interrupted   = 'INTERRUPTED'
unassigned  = 'UNASSIGNED'
# python mr_master.py <port> <data_dir>  


class ChunkInformation(object):
  def __init__(self, chunk_number, offset, length):
    self.chunk_number = chunk_number
    self.offset = offset
    self.length = length
    self.state = unassigned
    # IDEA: Will worker_number be the get_host_name of a given Bass Cluster worker
    #self.worker_number = worker_number

    def setState(self, state):
        self.state = state

    def addWorker(self, worker_number):
        pass
        #self.worker_number = worker_number
class Master(object):
  def __init__(self):
    gevent.spawn(self.controller)
    self.state = ready
    self.workers = {}
    self.Bookkeeper = {} #{'w1':['chunk1','working'], 'w2':['chunk2','finished']}
 
  # heartbeat between master and workers  
  def controller(self):
    while True:
      print '[Master:%s] ' % (self.state),
      for w in self.workers:
        print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0]),
      print
      for w in self.workers:
        try:
          res = self.workers[w][1].ping()
        except TimeoutExpired:
          print 'worker failure '+w[0]
          self.workers[w][0] = interrupted
      gevent.sleep(1) 

  def register_async(self, ip, port):
    print '[Master:%s] ' % self.state,
    print 'Registered worker (%s,%s)' % (ip, port)
    c = zerorpc.Client(timeout=1)  #set timeout for worker
    c.connect("tcp://" + ip + ':' + port)
    self.workers[(ip,port)] = [ready, c]
    c.ping()
  # called by worker for registration
  def register(self, ip, port):
    gevent.spawn(self.register_async, ip, port)

  #TODO called by worker's map() when the worker finishes mapping and askfor more tasks to map.
  def check_finish_map(self):
    print 'called by worker to check_finish_map'

  def ready_chunk_number(self):
    i = 0
    while( i < len(self.Bookkeeper)):
      if self.Bookkeeper[i].state == unassigned or self.Bookkeeper[i].state == interrupted:
        return i
      i = i + 1
    print 'No ready chunk numbers -- all are finished or currently working'
  
  def mapreduce(self, method_class, chunk_list, num_reducers):
    print 'in mapreduce in master'
    Bookkeeper = {}  #Create empty Bookkeeper (dict) 
    #TODO  wait for all mapping tasks finished an then master ask for all worker(alive) for partitioned files(if 2 reducers: every worker will have two files: one for reducer 1, one for reducer 2)

    # number of chunk that has been worked on
    procs = []
    c = 0 # counter: when c = len(chunk_list) mapper phase is done.
    while len(self.workers) < 1:
      gevent.sleep(2)
      print 'Waiting for workers to register' 
 
    # assign each worker a chunk(chunk_index) to work. 
    chunk_index  = 0;
    for w in self.workers:
      if chunk_index < len(chunk_list):
        chunk_data = chunk_list[chunk_index] 
        print 'worker '+str(w)+' is doing chunk'+str(chunk_index)
        chunk_stat = []   
        chunk_stat.append(chunk_index)
        chunk_stat.append(chunk_data)
        self.Bookkeeper[w] = chunk_stat
        proc = gevent.spawn(self.workers[w][1].map, method_class,chunk_data, num_reducers)
        print 'after map in mapreduce() '
        #remove chunk from chunk_list when finish the mapping of the chunk
        print 'before removing from chunk_list:'+ str(chunk_list)
        del chunk_list[chunk_index]
        print 'remove chunk #'+str(chunk_index)+' from chunk_list since it is mapped by worker'+str(w)
        print 'after removing from chunk_list:' + str(chunk_list)
        procs.append(proc)
    gevent.joinall(procs)
    print 'FINISH MAPPING' 
    procs = []  #clear the proc for reduce phase
    # start reducer, assume no workers die after they finish mapping and before reducers start reducing 
    num = 0 # find two reducers 
    for reducer in self.workers:
      if num <= (num_reducers-1):
        ips_mapper= self.get_ips_mappers(self.workers)
        input_file = 'wordcount'+str(num)+'.txt'
        num +=1 
        print '-----------------'
        print 'reducer is :'+str(self.workers[reducer][1])
        proc = gevent.spawn(self.workers[reducer][1].reduce, method_class, ips_mapper, input_file)
        procs.append(proc)
      else:
        break
    gevent.joinall(procs)
    print 'finish reducing and print out procs of reducing'  
    print len(procs)
    for p in procs:
      print p.value
    
  def get_ips_mappers(self, workers):
    ips = []
    for w in workers:
      ips.append(w[0]+':'+w[1])
    return ips

if __name__=='__main__':
  s = zerorpc.Server(Master())
  master_ip = '127.0.0.1'
  port = sys.argv[1]
  # TODO   data_dir = sys.argv[2]
  s.bind('tcp://'+ master_ip +':'+ port)
  s.run() 
  logging.basicConfig()
