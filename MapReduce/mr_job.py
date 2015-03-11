import zerorpc
import sys 
import os.path
import socket

import pickle

# python mr_job <ip_address_master:port> wordcount 100000 4 book.txt count
# python mr_job.py 0.0.0.0:4242 wordcount 10 3 test.txt count

class mr_job(object):
    def __init__(self):
      pass
    def build_chunk_list(self, method, split_size, inputfile):
      if method == "wordcount":

        offset = 0
        i = split_size
        # Number of bytes in input file
        input_file_bytes = os.path.getsize(inputfile)
        print 'number of bytes in input file is ', input_file_bytes , ' with type ',  type(input_file_bytes)
        print 'split size is ' , split_size , ' with type ',  type(split_size)

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
                chunk_list.append(chunk_data)
                offset = f.tell()
                print 'After adding chunk, new offset is ', offset
                i = offset + split_size
            
            else:
                i = i + 1
      elif method in ['hamming_enc','hamming_dec','hamming_err','hamming_chk','hamming_fix']:
        if method == 'hamming enc' and (split_size % 2 != 0):
          print 'split size should be multiple of 2'
        if method in ['hamming_dec','hamming_err','hamming_chk','hamming_fix'] and (split_size %3 !=
          0):
          print 'split size should be multiple of 3'
        # build chunk_list
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
            chunk_list.append(chunk_data)
            counter = counter + 1
    
    # Testing. 
      print ' After chunk list creation'
      j = 0
      while (j < len(chunk_list)):
        print ' Chunk ' , j , ' has length ' , chunk_list[j][1] , ' and offset ' , chunk_list[j][0]
        j = j + 1
        
      return chunk_list

if __name__ == '__main__':
  job = mr_job()
  master_addr = sys.argv[1] #<ipaddress:port>
  method = sys.argv[2]
  split_size = int(sys.argv[3])
  num_reducers = sys.argv[4]
  inputfile = sys.argv[5]
  outputBase = sys.argv[6]

    # Create a list of all chunks
  chunk_list = []
  f = open(inputfile)
  # build chunk list
  job.build_chunk_list(method,split_size,inputfile)
    
    #print 'chunk_list type is ' , type(chunk_list)
    #ip = socket.gethostbyname(socket.gethostname())
    #port = '4243'
    #s.bind('tcp://'+ip+':'+port)

  c = zerorpc.Client()
  # = zerorpc.Server()
  c.connect('tcp://'+ master_addr)
  result = c.do_job(method, chunk_list)
  #print 'SUCCESS !'
  #print result

'''
    if method == "wordcount":

        offset = 0
        i = split_size
        # Number of bytes in input file
        input_file_bytes = os.path.getsize(inputfile)
        print 'number of bytes in input file is ', input_file_bytes , ' with type ',  type(input_file_bytes)
        print 'split size is ' , split_size , ' with type ',  type(split_size)

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
                chunk_list.append(chunk_data)
                offset = f.tell()
                print 'After adding chunk, new offset is ', offset
                i = offset + split_size

            else:
                i = i + 1

    # Testing. 
    print ' After chunk list creation'

    j = 0
    while (j < len(chunk_list)):
        print ' Chunk ' , j , ' has length ' , chunk_list[j][0] , ' and offset ' , chunk_list[j][1]
        j = j + 1
    elif method == "hamming enc":
        offset = 0
        # Number of bytes in input file
        input_file_bytes = os.path.getsize(inputfile)

        counter = 0
        num_chunks = input_file_bytes / split_size
        length = split_size

        while (counter < num_chunks):

            offset = counter * split_size

            if counter == (num_chunks - 1):
                # Change the length to account for ending bytes
                length = input_file_bytes - offset

            chunk = Chunk(offset, length)
            chunk_list.append(chunk)
            counter = counter + 1
'''

    
