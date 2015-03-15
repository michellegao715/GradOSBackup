#  python mr_job.py master_ip:port [<name> | <mr_class.py>]  <split_size> <num_reducers> [<input_filename> | <base_filename>_] <output_filename_base>
import zerorpc
import sys 
import os.path
import socket


class split(object):
  chunk_list = []
  def __init_(self):
    pass
  def split_into_chunks(self,method,split_size,inputfile):
    if method == "wordcount":
        offset = 0
        i = split_size
        # Number of bytes in input file
        input_file_bytes = os.path.getsize(inputfile)
        print 'size of file is '+str(input_file_bytes)
        while i < input_file_bytes:
            chunk_data = []
            print 'inside while loop with i ' , i
            f.seek(i)
            if f.read(1) == '\n':
                print 'Found a new line at i ' , i
                length = f.tell() - offset
                #chunk = Chunk(offset, length)
                chunk_data.append(offset)
                chunk_data.append(length)
                self.chunk_list.append(chunk_data)
                offset = f.tell()
                print 'After adding chunk, new offset is ', offset
                if(offset+split_size < input_file_bytes):
                  i = offset+split_size
                else: #last chunk whose length < split_size 
                  chunk_data = []
                  last_offset = offset
                  last_length = input_file_bytes-last_offset
                  chunk_data.append(last_offset)
                  chunk_data.append(last_length)
                  self.chunk_list.append(chunk_data)
                  break
            else:
                i = i + 1 
    elif method in ['hamming_enc','hamming_dec','hamming_err','hamming_chk','hamming_fix']:
        if method == 'hamming enc' and (split_size % 2 != 0):
          print 'split size should be multiple of 2'
        if method in ['hamming_dec','hamming_err','hamming_chk','hamming_fix'] and (split_size %3 != 0):
          print 'split size should be multiple of 3'
        # build self.chunk_list
        offset = 0
        # Number of bytes in input file
        input_file_bytes = os.path.getsize(inputfile)
        counter = 0
        num_chunks = input_file_bytes / split_size
        length = split_size

        while (counter < num_chunks):
            chunk_data = []
            offset = counter * split_size
            if counter == (num_chunks - 1):
                # Change the length to account for ending bytes
                length = input_file_bytes - offset
            chunk_data.append(offset)
            chunk_data.append(length)
            self.chunk_list.append(chunk_data)
            counter = counter + 1
      # Testing. 
    print ' After chunk list creation'
    j = 0
    while (j < len(self.chunk_list)):
      print ' Chunk ' , j , ' has offset ' , self.chunk_list[j][0] , ' and length ' ,self.chunk_list[j][1]
      j = j + 1
        
    return self.chunk_list


if __name__ == '__main__':
  job = split()
  master_addr = sys.argv[1] #<ipaddress:port>
  method_class = sys.argv[2]
  split_size = int(sys.argv[3])
  num_reducers = sys.argv[4]
  input_file = sys.argv[5]
  output_base = sys.argv[6]

  f = open(input_file)
  chunk_list = job.split_into_chunks(method_class,split_size,input_file)
  
  # pass mapreduce tasks(self.chunk_list, #ofReducers, method_class) to master. 
  '''c = zerorpc.Client()
  c.connect("tcp://"+master_addr)
  result = c.do_job(method_class, self.chunk_list, num_reducers) '''

  

  
