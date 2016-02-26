#!/usr/bin/python

import sys

last_key = None
last_label = None
fea_list = ["1"]
uDict = {}
for line in sys.stdin :
    var = line.strip().split("\t")
    if len(var) != 3 :
        print "error: %s" % (line.strip())
        continue

    (key, signal, value) = line.strip().split("\t")
    #print key,signal,value

    if last_key != key :
        if last_key and len(fea_list)>1 and last_label!=None:
            fea_list.sort(key=lambda x:int(x))
            print "%s\t%d:%s" % (last_label, len(fea_list), "_".join(fea_list))
        last_label = None
        fea_list = ["1"]
        last_key = key
        uDict = {} 
    if signal == '1' :
        last_label = value
    elif signal == '2' :
        #if (key == last_key) :
        if(value not in uDict):
            fea_list.append(value)
            uDict[value] = 1

if last_key and len(fea_list)>1 and last_label!=None:
    fea_list.sort(key=lambda x:int(x))
    print "%s\t%d:%s" % (last_label, len(fea_list), "_".join(fea_list))
