import sys
import socket
import zerorpc
from zerorpc.exceptions import TimeoutExpired
import gevent


ready  = 'READY'
working  = 'WORKING'
finished    = 'FINISHED'
interrupted   = 'INTERRUPTED'

# python mr_master.py <port> <data_dir>  
class Master(object):
  map_finished = False #start reducer when all chunks are mapped. 
  
  def __init__(self):
    gevent.spawn(self.controller)
    self.state = ready
    self.workers = {}
 
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
 


  def finish_map(self):
    map_finished = True
  
  def mapreduce(self, method_class, chunk_list, num_reducers):
    print 'in mapreduce in master'
    #TODO  wait for all mapping tasks finished an then master ask for all worker(alive) for partitioned files(if 2 reducers: every worker will have two files: one for reducer 1, one for reducer 2)
    
    # number of chunk that has been worked on
    c = 1
    procs = []
    while True:
      if c > len(chunk_list):
        break
      for w in self.workers:
        if c <= len(chunk_list) and w[0] == ready:
          print 'worker '+str(w)+' is doing job'+str(c)
          proc = gevent.spawn(self.workers[w][1].map(), method_class, chunk_list[c], num_reducers)
          procs.append(proc)
          c += 1 
    gevent.joinall(procs)
    print 'finish mapping '
    # TODO test the result of proc
    print 'print out everything in procs of mapping'  
    for p in procs:
      print p.value
   
    # start reducer, assume no workers die after they finish mapping and before reducers start reducing 
    for r in range(num_reducers):
      reducer = self.workers[r]
      print 'worker '+str(r)+' works as reducer'
      ips_mapper= get_ips_workers(self.workers)
      # intermediate file of mapper is output1, output2....
      input_file = 'output'+str(r+1)+'.txt'
      proc = gevent.spawn(self.workers[r][1].reduce(), ips_mapper, input_file)
      procs.append(proc)
    gevent.joinall(procs)
    print 'finish reducing' 
    for p in procs:
      print p.value

  def get_ips_mappers(self, workers):
    ips = []
    for w in workers:
      ips.append(w[0])
    return ips

if __name__=='__main__':
  s = zerorpc.Server(Master())
  master_ip = '127.0.0.1'
  port = sys.argv[1]
  # TODO   data_dir = sys.argv[2]
  s.bind('tcp://'+ master_ip +':'+ port)
  s.run() 
