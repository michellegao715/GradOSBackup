#!/usr/bin/env python
import sys 

'''
def dataset(path):
    with open(path, 'rU') as data:
        reader = csv.reader(data)
        for row in reader:
            row[2] = int(row[2])
            yield row
'''
job=10
j = 1
while True:
  if j > job:
    break
  for w in [1,2,3]:
    if j <= job:
      print 'worker '+str(w)+' is doing job '+str(j)
      j = j+1
    else:
      break
print 'finish all works'

