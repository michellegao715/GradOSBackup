import sys

import socket
import zerorpc
import gevent
import mapreduce
import itertools
import job


STATE_UNASSIGNED     = 'UNASSIGNED'
STATE_WORKING    = 'WORKING'
STATE_FINISHED    = 'FINISHED'
STATE_INTERRUPTED   = 'INTERRUPTED'


class ChunkInformation(object):

    def __init__(self, chunk_number, offset, length):
        self.chunk_number = chunk_number
        self.offset = offset
        self.length = length
        self.state = STATE_UNASSIGNED
        # IDEA: Will worker_number be the get_host_name of a given Bass Cluster worker
        #self.worker_number = worker_number

    def changeState(self, state):
        self.state = state

    def addWorker(self, worker_number):
        pass
        #self.worker_number = worker_number

class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.workers = {}

    def controller(self):
        while True:
            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0]),
            print
            for w in self.workers:
              try:
                self.workers[w][1].ping()
              except e:
                print 'exception when ping workers' 
            gevent.sleep(1)

    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = ('READY', c)
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)

    def do_job(self, mapreduce_method, chunk_list):

        #assert isinstance(chunk_list, list)
        #chunk_list = pickle.loads(chunk_list)

        print 'Entered Master do_job'
        # Add chunk_list to our Bookkeeper
        Bookkeeper = []


        j = 0
        while (j < len(chunk_list)):

            chunk_info = ChunkInformation(j, chunk_list[j][0], chunk_list[j][1])
            Bookkeeper.append(chunk_info)
            j = j + 1

        print 'Created Bookkeeper'

        k = 0
        while (k < len(Bookkeeper)):
            print 'Row ' , k , 'has chunk number : ', Bookkeeper[k].chunk_number , ' with offset' , Bookkeeper[k].offset , ' with length ' , Bookkeeper[k].length , 'with state ' , Bookkeeper[k].state
            k = k + 1

        # TODO: Add a finished job method. This will iterate through all rows in "Bookkeeper"
        # and if all states are Finished, it will return TRUE

        n = len(self.workers)
        #values = f.readlines()
        # every worker works on one chunk
        #chunk = len(values) / n
        i = 0 # index of worker
        offset = 0 # position of file that has been arranged for worker. 
        #result = 0
        #procs = []
        #for w in self.workers:
        #    print 'there are '+str(n)+' workers in all'
        #    if i == (n - 1):
        #        print 'last worker'
        #        sub = values[offset:]
        #    else:
        #        sub = values[offset:offset+chunk]
        #    proc = gevent.spawn(self.workers[w][1].do_work, mapreduce_method, sub)
        #    procs.append(proc)
#
        #    #result += int(self.workers[w][1].do_work(sub))
        #    i = i + 1
        #    offset = offset + chunk

        #gevent.joinall(procs)
        #return sum([int(p.value) for p in procs])
        #return list(itertools.chain(*procs)) 
        res = [] 
        # Combine result: sum value of same key. 
        #for l in procs:
        #  res.append(l.value)
        #return res


if __name__ == '__main__':
  s = zerorpc.Server(Master())
  master_ip = '127.0.0.1'
  port = sys.argv[1]
  # TODO   data_dir = sys.argv[2]
  s.bind('tcp://'+ master_ip +':'+ port)
  s.run()

