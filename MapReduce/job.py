import sys
import mapreduce
class WordCount(object):
  def __init__(self, input_chunk, map_class, reduce_class):
    self.input_chunk = input_chunk
    self.map_class = map_class
    self.reduce_class = reduce_class 
    self.result_list = None 

class WordCountMap(mapreduce.Map):
    def __init__(self):
      self.table = {}
    def map(self, k, v):
      words = v.split()
      for w in words:
        self.emit(w, '1')
    def emit(self, k, v):
      if k in self.table:
        self.table[k].append(v)
      else:
        self.table[k] = [v]
      
      ''' split mapped(emmitted) results to different reducers:
      {'apple':[1,1,1], 'pear':[1,1,1], 'orange':[1,1], 'melon':[1,1]}  --> two reducers:  r1:(apple,melon)  r2:(pear,orange) 
        '''
    def partition(self,k,num_reducers):
      # TODO overrdie hash() 
      return hash(k)%num_reducers   
    def get_table(self):
      return self.table
class WordCountReduce(mapreduce.Reduce):
    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))

class HammingEncodeMap(mapreduce.Map):
  pass 
class HammingEncodeReduce(mapreduce.Reduce):
  pass