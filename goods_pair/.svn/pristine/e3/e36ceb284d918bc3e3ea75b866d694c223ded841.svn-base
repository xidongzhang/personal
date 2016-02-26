#! /usr/bin/python
import sys
import os

num = 0
#input_file="part-00068.lzo_deflate"
input_file=os.environ['map_input_file']
file_idx = (input_file.strip().split("-")[-1]).split(".")[0]

for line in sys.stdin :
	num += 1

print "%s\t%d" % (file_idx, num)
