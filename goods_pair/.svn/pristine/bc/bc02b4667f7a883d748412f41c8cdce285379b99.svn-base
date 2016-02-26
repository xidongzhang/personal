#!/usr/bin/python

import sys

last_key = None
last_idx = 0

for line in sys.stdin :
    (key, signal, value) = line.strip().split("\t")
    #print key, signal, value
    #if last_key != key :
    #    if last_key :
    #        print last_key
    #    last_key = key
    
    if signal == '1' :
        last_idx = value
        last_key = key
        #print "%s\t%s\t3" % (key, value)
    elif signal == '2' :
        #print "%s\t%s\t%s\t4" % (var[2],last_idx, var[0])
        if (key != last_key) :
            msg = "%s : not matched" % (line.strip())
            #print >>stderr,"%s : not matched" % (line.strip())
        else :
            print "%s\t%s\t4" % (value, last_idx)
            #arr = value.split("_")
            #for sign in arr:
            #print "%s\t%s\t%s\t4" % (key, sign, last_idx)
