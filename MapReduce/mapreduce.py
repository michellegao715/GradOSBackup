# mapreduce.py

class Map(object):

    def __init__(self):
        self.table = {}

    def map(self, k, v):
        pass

    def emit(self, k, v):
        if k in self.table:
            self.table[k].append(v)
        else:
            self.table[k] = [v]

    def get_table(self):
        return self.table


class Reduce(object):

    def __init__(self):
        self.result_list = []

    def reduce(self, k, vlist):
       pass

    def emit(self, v):
        self.result_list.append(v)
        #print 'this is appended '+str(v)

    def get_result_list(self):
        return self.result_list


class Engine(object):

    def __init__(self, input_list, map_class, reduce_class):
        self.input_list = input_list
        self.map_class = map_class
        self.reduce_class = reduce_class
        self.result_list = None

    def execute(self):

        mapper = self.map_class()
        # Map phase
        for i, v in enumerate(self.input_list):
          #print 'i is '+str(i)
          #print 'v is '+v
          mapper.map(i, v)

        # Sort intermediate keys
        table = mapper.get_table()
        # print str(table)
        keys = table.keys()
        keys.sort()

        # Reduce phase
        reducer = self.reduce_class()
        for k in keys:
          reducer.reduce(k, table[k])
         # print 'key is :'+k
            #print i+' is element in list table[k]'
        self.result_list = reducer.get_result_list()

    def get_result_list(self):
        return self.result_list

  





