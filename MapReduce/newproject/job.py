import sys
import mapreduce
import hamming 

class WordCount(object):
  def __init__(self, input_chunk, map_class, reduce_class):
    self.input_chunk = input_chunk
    self.map_class = map_class
    self.reduce_class = reduce_class 
    self.result_list = []
  def get_result_list(self):
    return self.result_list
class WordCountMap(mapreduce.Map):
    def __init__(self):
      self.table = {}
    def map(self, k, v):
      #words = v.split()
      #for w in words:
      self.emit(v, '1')
    
    def emit(self, k, v):
      if k in self.table:
        self.table[k].append(v)
      else:
        self.table[k] = [v]
      
      ''' split mapped(emmitted) results to different reducers:
      result of one mapper: 
      {'apple':[1,1,1], 'pear':[1,1,1], 'orange':[1,1], 'melon':[1,1]}  
      --> two reducers:  r1:(apple,melon)  r2:(pear,orange)  ''' 
    def partition(self,k,num_reducers):
      # TODO  partition keys to different reducers 
      return hash(k)%num_reducers   
    def get_table(self):
      return self.table
class WordCountReduce(mapreduce.Reduce):
    def __init__(self):
      self.result_list = []
    def reduce(self, k, vlist):
      count = 0
      for v in vlist:
        count = count + int(v)
      self.emit(k + ':' + str(count))
      
    def emit(self,v):
      self.result_list.append(v)
    
    def get_result_list(self):
      return self.result_list

class HammingEncodeMap(hamming.HammingBinary):
    def __init__(self):
      self.table = {}
    def map(self, k, v):
      self.emit(k,self.encode(v))
  # encode one character to 12 bytes 10001...01
    def emit(self, k, v):
      if k in self.table:
        self.table[k].append(v)
      else:
        self.table[k] = [v]
class HammingEncodeReduce(hamming.HammingBinary):
    def __init__(self):
      self.result_list = []
    def reduce(self, k, vlist):
      self.emit(k,vlist[0])
    
    def emit(self,v):
      self.result_list.append(v)

    def get_result_list(self):
      return self.result_list


