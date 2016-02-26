#!/usr/bin/python

import sys

last_key = None
num = 0

for line in sys.stdin :
    key = (line.strip().split("\t"))[0];
    if last_key != key :
        if last_key :
            print last_key, num
        last_key = key
        num = 0
    num += 1

if last_key :
    print last_key, num
