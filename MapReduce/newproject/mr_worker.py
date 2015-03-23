import sys

import zerorpc
import gevent
import socket
import job
import string 

master_addr="tcp://127.0.0.1:4242"
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

  # chunk_data: <offset, length> 
  def map(self, method_class, chunk_data, num_reducers):
    if method_class == 'wordcount':
      mapper = job.WordCountMap()
    #TODO copy do_work from job.py of old version 
    # write intermediate file into count_xx.txt 
    chunk = self.fetch_chunk(method_class, chunk_data)
    # map
    for i,v in enumerate(chunk):
      mapper.map(i,v)
    map_table = mapper.get_table()
    map_keys = map_table.keys()
    print 'keys :'+str(map_keys)
    # write to intermediate_file 
    out_files = self.create_file(method_class, num_reducers) 
    fs = []  # Open all intermediate files and write <key,vlist> to them
    for f in out_files:
      out_file = open(f,'w')
      fs.append(out_file)
    for key in map_keys:
      fs[hash(key)%num_reducers].write(key + '\n'+ '\t'.join(map_table[key])+'\n')
    #Ask for extra chunk after finish current chunk and if there is still left in chunk_list
    c = zerorpc.Client()
    c.connect(master_addr)
    c.check_finish_map()

  def create_file(self, method_class, num_reducers):
    fs = []
    for i in range(num_reducers):
      f = method_class+str(i)+'.txt'
      fs.append(f)
    return fs

  # TODO ask every worker in ips_mapper for interdemiate file(each ip_mapper should have num_reducers' intermediate files
  def reduce(self, method_class, ips_mapper, input_file):
    print 'in reduce function of mr_worker'
    if method_class == 'wordcount':
      reducer = job.WordCountReduce()
    table = {}
    for ip_mapper in ips_mapper:
      #TODO ask for input_file from ip_mapper 
      print 'try to ask input file from ip_mapper'
      c = zerorpc.Client()
      c.connect('tcp://'+ip_mapper)
      lines = c.get_file(input_file)
      # even line(line 0,2,4....) is key, odd line(line 1,3,5...) is vlist
      # Prepare table{<k,vlist>,<k,vlist>,....} for reducing 
      line = ' '
      i = 0
      lines = map(lambda s: s.strip(), lines)
      while i < len(lines) and (len(line) > 0):
        line = lines[i]
        i+=1
        key = line
        print 'key is '+str(key)
        line = lines[i]
        i+=1
        vlist = line.split('\t')
        if len(line) == 0:
          break
        print 'vlist is '+str(vlist)
        if key in table:
          table[key].append(vlist)
        else:
          table[key] = vlist
    keys = table.keys()
    for k in keys:
      print 'the vlist for '+k+' is:'+str(table[k])
      reducer.reduce(k, table[k])          
    result_list = reducer.get_result_list()
    print 'after reducing:'+str(result_list)
    return result_list
  
  def get_file(self, input_file):
    f = open(input_file, 'r')
    lines = f.readlines()
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

