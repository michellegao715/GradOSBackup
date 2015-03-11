import sys

import zerorpc
import gevent

master_addr = 'tcp://0.0.0.0:4242'

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
                self.workers[w][1].ping()

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

    def do_job(self, nums):
        n = len(self.workers)
        chunk = len(nums) / n
        i = 0
        offset = 0
        #result = 0
        procs = []
        for w in self.workers:
            if i == (n - 1):
                sub = nums[offset:]
            else:
                sub = nums[offset:offset+chunk]

            proc = gevent.spawn(self.workers[w][1].do_work, sub)
            procs.append(proc)

            #result += int(self.workers[w][1].do_work(sub))
            i = i + 1
            offset = offset + chunk

        gevent.joinall(procs)
        return sum([int(p.value) for p in procs])

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

    def do_work(self, nums):
        nums = [int(n) for n in nums]
        gevent.sleep(2)
        return str(sum(nums))


if __name__ == '__main__':

    cmd = sys.argv[1]

    if cmd == 'master':
        s = zerorpc.Server(Master())
        s.bind(master_addr)
        s.run()
    elif cmd == 'worker':
        s = zerorpc.Server(Worker())
        ip = '0.0.0.0'
        port = sys.argv[2]
        s.bind('tcp://' + ip + ':' + port)
        c = zerorpc.Client()
        c.connect(master_addr)
        c.register(ip, port)
        s.run()
    elif cmd == 'client':
        c = zerorpc.Client()
        c.connect(master_addr)
        result = c.do_job(sys.argv[2:])
        print result






