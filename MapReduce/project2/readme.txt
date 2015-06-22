Implementation:
mr_job:
split_into_chunks():
Splitting whole file by split_size(based on  mapreduce operation such as wordcount and hamming encode). As for wordcount, it will try split by split_size and then keep reading until meet a newline to get a chunk. 
The chunk_list constructed is: [ [offset1,length1], [offset2, length2] ... ]
As for hamming encode/decode, check the split_size is mulitple of 2 for encode and 3 for decode/error/check/fix. 
Then call the mr_master's mapreduce() function to start mapreducing. 

mr_master:
The way to assign chunks(from chunk_list) to workers: 
First assign each worker a chunk_data to map, when a worker finish its task, it will call mr_master's check_map_finish() to check if there is still mapper tasks left, if yes then master will give the worker a task to do; if not, start the reduce part.  This way, it will avoid busy wait of master and assign the rest of tasks to available workers. 

bookkeeper: a dictionary of the format: {worker:[[chunk_data1, status1],[chunk_data1, status]], ....} which indicates which worker is working on which chunk and the status of the chunk(working, finished)
Update Bookkeeper and chunk_list in mr_master when a worker dies, assign a chunk to worker and  worker finished a chunk. 

controller():
heartbeat to ping workers and maintain self.workers as available workers. If there is timeout which means not receiving a worker's feedback, it will set the worker to be interrupted status and reschedule the job of the worker to another worker. 

register():
when all workers are lauched, they will call register() of master and master will save their ip+port to self.workers 

mapreduce_reduce():
Reducing stage, start reducing when finishing mapping all chunks in chunk_list.

mapreduce():
Mapping stage, given the chunk_list(from mr.job) , method_name and number of reducers, master start assigning map tasks to workers. All workers will be assigned one chunk at first, and then if anyone of them finish the mapping, it calls the master's check_finish_map() to see if there is any chunks left to be mapped. if not, the master start reducing, else the worker will be given a chunk to map.  

reschedule_job():
Handle worker(mapper) failure. update the chunk_list if assigning a chunk of a dead worker to another worker alive, and update the bookkeeper which save the information of which worker(ip,port) is handling which chunk and the working status(running, done).  


mr_worker:
Worker will do map or reduce tasks based on what master tell it. Master will pass the method_name, chunk_data to work on, and num_reducers to map() function of worker, then worker will write the intermediate file(each assigned chunk has number of reducers' output files of mapping)  into disks of following format: 
word1 
1 1 1 
word2 
1 1 
...  
reduce():
The worker will be called to reduce given the method_name and intermediate files. It will read line by line from the intermediate files and fill in the result_list in the format of:
[word1:3, word2:2,.....]  

controller():
heartbeat, print out 'Worker'
ping():
called by master's register() to make sure worker is alive. 


Usage: 
terminal 1: python mr_master.py 4242 .
terminal 2: python mr_worker.py 127.0.0.1:4242 4242 
terminal 3: python mr_job.py 0.0.0.0:4242 hamming 14 2 
