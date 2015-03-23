import sys

class HammingFile(object):

  def __init__(self, infile):
    self.infile = infile

  def get_ascii(self, byte_str):
    data = byte_str[0:8]
    value = int(data, 2)
    return chr(value) 
  
  def getParityPositions(self,c):
    p=[1]
    s="{0:08b}".format(ord(c))
    a=2
    while(a<=len(s)):
      p.append(a)
      a*=2
    return p

  # Get sequence of bits to be checked given the position of parity bit.
  def checking_bits(self,i):
    position = []
    a = i
    while a <= 12:
      for x in range(i):
        if(a<=12):
          position.append(a)
          a+=1
      a+=i
    return position

  # calculate hamming code:  _ _ 1 _ 0 0 1 _ 1 0 1 0, p is the position to set. hammingcode has length of 12. 
  def get_parity(self, p, hammingcode):
    checkingbits = self.checking_bits(p)
    sum = 0
    for b in checkingbits:
      if hammingcode[b-1] == '1':
        sum+=1
    return sum%2
   
  # encode one character to 12 bytes 10001....01
  def encode(self,c):
    positions = self.getParityPositions(c) #get 1,2,4,8 to put parity bit  
    bin_str="{0:08b}".format(ord(c))
    hammingcode = [None]*(len(bin_str)+len(positions))
    for i in positions:
      # set parity bit to 0 at position 1,2,4,8
      hammingcode[i-1] = '0'
    # add original data to hammingcode
    j = 0
    for c in hammingcode:
      if(c == None and j<8):
        hammingcode[hammingcode.index(c)]=bin_str[j]
        j+=1
    # set hamming code to position 1,2,4,8 
    for p in positions:
      hammingcode[p-1] = str(self.get_parity(p,hammingcode))
    #print hammingcode
    return ''.join(hammingcode)
    """expected 12 length hammingcode""" 

  '''pass in multiple 12 bytes hamming code and decode to original char. let ascii/bin encode to exten
  it do appropriate read. delete 1,2,4,8 element ''' 
  def decode(self, text):
    out_string = ''
    while len(text)>0:
      if text[0] == '\n':
        break
      byte_str = text[0:12]
      s = byte_str[2]+byte_str[4:7]+byte_str[8:12]
      text = text[12:]
      value = int(s,2)
      out_string += chr(value)
    print 'decoded text :' + out_string
    return out_string

  # check n times of 12 bits of hamming code. 
  def check(self,text):
    c = 'a' 
    positions = [] 
    err_pos = 0
    counter = 0  # number of 12-length hamming code has been checked 
    while len(text) > 0:
      if text[0] == '\n':
        break
      char_str = text[0:12]
      text = text[12:]
      err_pos = 12*counter
      flag = True  # flip it if find error parity bit.
      for pos in self.getParityPositions(c):
        sum = 0
        for i in self.checking_bits(pos):
          sum += int(char_str[i-1])
        if sum%2 != 0:
          flag = False 
          err_pos += pos
      if flag == False:
        positions.append(err_pos)
      counter+=1
    return positions 

  def fix(self,input_text):
    error_positions = self.check(input_text)
    output_text = ''
    i = 1
    while i <= len(input_text):
      if i in error_positions:
        output_text += str(1-int(input_text[i-1]))
      else:
        output_text += input_text[i-1]
      i+=1
    return output_text

  def err(self,pos,text):
    p = int(pos)-1
    if text[p] == '1':
      newtext = text[0:p]+'0'+text[(p+1):]
    elif text[p] == '0':
      newtext = text[0:p]+'1'+text[(p+1):]
    return newtext

class HammingAscii(HammingFile):
  def __init__(self,in_filename,out_filename=''):
    self.in_filename = in_filename
    self.out_filename = out_filename 
  # testing hamming ascii 
  def encode_file(self):
    infile = open(self.in_filename)
    outfile = open(self.out_filename, 'w')
    text = infile.read()
    for t in text:
      encoded_t = self.encode(t)
      outfile.write(str(encoded_t))
    infile.close()
    outfile.close()

  def decode_file(self):
    infile = open(self.in_filename)
    text = infile.read()
    # ascii decode: 10001...1  --> string
    decoded_text = self.decode(text)
    outfile = open(self.out_filename, 'w')
    outfile.write(decoded_text)
  
  def check_file(self):
    infile = open(self.in_filename)
    text = infile.read()
    return self.check(text)
  
  def fix_file(self):
    infile = open(self.in_filename)
    outfile = open(self.out_filename,'w')
    text = infile.read()
    newtext = self.fix(text)
    outfile.write(newtext)

  def err_file(self,pos):
    infile = open(self.in_filename)
    text = infile.read()
    newtext = self.err(pos,text)
    outputfile = open(self.out_filename, 'w')
    outputfile.write(newtext)

class HammingBinary(HammingFile):
  def __init__(self,in_filename,out_filename=''):
    self.in_filename = in_filename
    self.out_filename = out_filename
  def check_file(self):
    infile = open(self.in_filename)
    text = infile.read()
    binary_strs = ''
    for c in text:
      binary_str = "{0:08b}".format(ord(c))
      binary_strs += binary_str
    size = len(binary_strs)/12*12 
    binary_strs = binary_strs[0:size]
    return self.check(binary_strs)

  def fix_file(self):
    infile = open(self.in_filename)
    text = infile.read()
    strs = ''
    for c in text:
      binary_str = "{0:08b}".format(ord(c))
      strs += binary_str
    new_strs = self.fix(strs)
    self.save(new_strs)

  def err_file(self,pos):
     infile = open(self.in_filename)
     text = infile.read()
     # convert ascii string to binary string 
     binary_strs = ''
     for c in text:
       binary_str = "{0:08b}".format(ord(c))
       binary_strs += binary_str 
     # convert into ascii string 
     new_text =  self.err(pos,binary_strs)
     self.save(new_text)
  # write 01010...1  to a^( ...  
  def save(self,binary_strs):
    text = ''
    outfile = open(self.out_filename,'w')
    while len(binary_strs) > 0:
      data = binary_strs[0:8]
      binary_strs = binary_strs[8:]
      value = int(data,2)
      text += chr(value)
    outfile.write(text)

  def encode_file(self):
    infile = open(self.in_filename)
    outfile = open(self.out_filename,'w')
    out_string = ''
    text = infile.read()
    for t in text:
      encoded_t = self.encode(t)
      out_string += encoded_t
    if len(out_string)%8 != 0:
      out_string += '0000'
    while len(out_string) > 0:
      char_str = out_string[0:8]
      out_string = out_string[8:]
      outfile.write(chr(int(char_str,2)))
  def decode_file(self):
    infile = open(self.in_filename)
    outfile = open(self.out_filename,'w')
    text = infile.read()
    binary_strs = ''
    for c in text:
      binary_str = "{0:08b}".format(ord(c))
      binary_strs += binary_str
    size = len(binary_strs)/8*8
    binary_strs = binary_strs[0:size]
    decoded_text = self.decode(binary_strs)
    outfile.write(decoded_text) 
if __name__ == '__main__':
  encodetype = sys.argv[1]
  cmd = sys.argv[2]
  # infile, outfile. 
  if len(sys.argv) == 4:
    infile = sys.argv[3]
  if len(sys.argv) == 5:
    infile = sys.argv[3]
    outfile = sys.argv[4]
  if len(sys.argv) == 6:
    pos = sys.argv[3]
    infile = sys.argv[4]
    outfile = sys.argv[5]
  
  if encodetype == 'bin':
    if cmd == 'enc':
      coder = HammingBinary(infile,outfile)
      coder.encode_file()
    if cmd == 'dec':
      coder = HammingBinary(infile,outfile)
      coder.decode_file()
    if cmd == 'chk':
      coder = HammingBinary(infile)
      print coder.check_file()
    if cmd == 'err':
      coder=  HammingBinary(infile,outfile)
      coder.err_file(pos)
    if cmd == 'fix':
      coder = HammingBinary(infile,outfile)
      coder.fix_file()
  if encodetype == 'asc':
    if cmd == 'enc':
      coder = HammingAscii(infile,outfile)
      coder.encode_file()
    if cmd == 'dec':
      coder = HammingAscii(infile,outfile)
      coder.decode_file()
    if cmd == 'chk':
      coder = HammingAscii(infile)
      print coder.check_file()
    if cmd == 'fix':
      coder = HammingAscii(infile,outfile)
      coder.fix_file()
    if cmd == 'err':
      coder = HammingAscii(infile,outfile)
      coder.err_file(pos)
