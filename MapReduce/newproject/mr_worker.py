import sys

import zerorpc
import gevent
import socket
import job
import string 

#master_addr="tcp://127.0.0.1:4242"
#master_addr="tcp://10.0.0.25:4242"
master_addr="tcp://76.103.14.70:4242" 
# python mr_worker.py <ip_address_master:port>
class Worker(object):
  ''' def __init__(self, chunk_data, mapreduce_method):
        gevent.spawn(self.controller)
        self.mapreduce_method = mapreduce_method
        self.chunk_data= chunk_data
        self.result_list = None   '''
  def __init__(self, ip, port):
    self.ip = ip
    self.port = port 

  def controller(self):
    while True:
      print('[Worker]')
      gevent.sleep(1)

  def ping(self):
    print('[Worker] Ping from Master') 

  # chunk_data: <offset, length> 
  def map(self, method_class, chunk_data, num_reducers):
    c = zerorpc.Client()
    c.connect(master_addr)
    # while(check_finish_map(self.ip, self.port) != None):
    while(chunk_data != False):
      print 'CAN KILL WORKER NOW TO HANDLE WORKER FAILURE' 
      gevent.sleep(3)
      print "WORKING"
      if method_class == 'wordcount':
        mapper = job.WordCountMap()
      if method_class in ['hamming_enc','hamming_dec','hamming_err','hamming_chk','hamming_fix']:
        mapper = job.HammingEncodeMap()

      # write intermediate file into count_xx.txt 
      chunk = self.fetch_chunk(method_class, chunk_data)
      # map
      for i,v in enumerate(chunk):
        mapper.map(i,v)
      map_table = mapper.get_table()
      map_keys = map_table.keys()
      #print 'keys :'+str(map_keys)
      # write to intermediate_file 
      out_files = self.create_file(method_class, num_reducers, chunk_data[0]) 
      fs = []  # Open all intermediate files and write <key,vlist> to them
      for f in out_files:
        out_file = open(f,'w')
        fs.append(out_file)
      for key in map_keys:
        fs[hash(key)%num_reducers].write(key + '\n'+ '\t'.join(map_table[key])+'\n')
      #Ask for extra chunk after finish current chunk and if there is still left in chunk_list
      chunk_data = c.check_finish_map(self.ip, self.port)
    print "DONE MAPPING"
    return ''


  def create_file(self, method_class, num_reducers, offset):
    fs = []
    for i in range(num_reducers):
      f = method_class+str(i)+'_'+str(offset)+'.txt'
      fs.append(f)
    return fs

  # `ask every worker in ips_mapper for interdemiate file(each ip_mapper should have num_reducers' intermediate files
  def reduce(self, method_class, file_locations):
    print 'in reduce function of mr_worker'
    print file_locations
    if method_class == 'wordcount':
      reducer = job.WordCountReduce()
    if method_class == 'hamming':
      reducer = job.HammingEncodeReduce()
    table = {}
    # file_locations of intermediate files: ['wordcount0_0.txt', wordcount0_106.txt',...] Name the reduce output file to be wordcount0.txt and write the result_list which is the result of reducing to it.
    #print 'file_locations[0]: '+str(file_locations[0])
    #print 'file_locations[0][0]:'+str(file_locations[0][0])
    r = file_locations[0][1][0].index('_') 
    reduce_file = file_locations[0][1][0][:r] 
    print '-----------------------------'
    print 'name of reduce_file is '+reduce_file
    for (worker, locations) in file_locations:
      print 'try to ask intermediate file from mapper'
      lines = self.get_file(worker, locations)
      # even line(line 0,2,4....) is key, odd line(line 1,3,5...) is vlist
      # Prepare table{<k,vlist>,<k,vlist>,....} for reducing 
      line = ' '
      i = 0
      lines = map(lambda s: s.strip(), lines)
      print lines
      while i < len(lines) and (len(line) > 0):
        key = lines[i]
        i+=1
        vlist = lines[i].split('\t')
        i+=1
        if len(line) == 0:
          break
        if key in table:
          table[key] += vlist
        else:
          table[key] = vlist
    keys = table.keys()
    for k in keys:
      print 'the vlist for '+k+' is:'+str(table[k])
      reducer.reduce(k, table[k])          
    result_list = reducer.get_result_list()
    reduce_f = open(reduce_file, 'w') 
    reduce_f.write(str(result_list))
    reduce_f.close()
    print 'after reducing:'+str(result_list)
    return result_list
  
  def get_file(self, worker, locations):
    print "GET FILE"
    if worker[0] == self.ip and worker[1] == self.port:
      return self.read_file(locations)
    else:
      c = zerorpc.Client()
      print 'the worker mapper is :'+str(worker)
      print 'worker ip is :'+str(worker[0])
      print 'worker port is :'+str(worker[0])
      
      c.connect('tcp://' + worker[0]+':'+worker[1])
      return c.read_file(locations)

  def read_file(self, locations):
    print "READ FILE"
    lines = []
    for location in locations:
      with open(location) as f:
        lines += f.readlines()
    return lines

  def return_reduced_file(self, input_file):
    f = open(input_file, 'r')

  def fetch_chunk(self, mapreduce_method, chunk_data):
    in_file = open('StarSpangledBanner.txt','rb')  
    offset = chunk_data[0]
    length = chunk_data[1]
    start = in_file.read(offset)
    if mapreduce_method == 'wordcount':
      #read by bytes and convert to list of words(split by " ", remove all punctuations) 
      raw_data = in_file.read(length).replace('\n',' ')
      print 'chunk is '+str(raw_data)
      #remove punctuation 
      str_data = "".join(l for l in raw_data if l not in string.punctuation)
      #remove empty string from str_data 
      data = filter(None, str_data.split(' '))
      print 'reading from file for word count :'+str(data) 
    else:
      data = in_file.read(length)
    return data


if __name__ == '__main__':
  # ip = socket.gethostbyname(socket.gethostname())
  ip = sys.argv[1]  # type in the worker's ip
  port = '4243'
  #TODO pass method and chunkdata for testing
  #method = 'wordcount'
  s = zerorpc.Server(Worker(ip,port))
  print 'after open new Worker:'+str(ip)+':'+str(port)
  s.bind('tcp://'+ip+':'+port)
  c = zerorpc.Client()
  print 'before connect to master'
  c.connect(master_addr)
  print 'after connect to master'
  c.register(ip,port)
  s.run()

