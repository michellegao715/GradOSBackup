f = open('test1.txt', 'r')
line = ' '
while len(line) > 0:
  line = f.readline()[:-1]
  if len(line) == 0:
    break
  print line
f.close()
