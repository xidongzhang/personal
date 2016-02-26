#!/usr/bin/python

import sys

last_key = None
last_label = None
fea_list = []

for line in sys.stdin :
    var = line.strip().split("\t")
    if len(var) != 3 :
        print "error: %s" % (line.strip())
        continue

    (key, signal, value) = line.strip().split("\t")
    #print key,signal,value

    if last_key != key :
        if last_key and last_label!=None:
            fea_list.sort(key=lambda x:int(x))
            print "%s\t%d:%s\t%s" % (last_label, len(fea_list), "_".join(fea_list), last_key)
        last_key = key
        fea_list = ['1']
        last_label = None

    if signal == '1' :
        last_label = value
    elif signal == '2' :
        fea_list.append(value)

if last_key and last_label!=None:
    fea_list.sort(key=lambda x:int(x))
    print "%s\t%d:%s\t%s" % (last_label, len(fea_list), "_".join(fea_list), last_key)
