#!/usr/bin/python

import sys

for line in sys.stdin :
    var = line.strip().split("\t")
    if (var[-1] == '4'): # fea cnt idx 4
        print "%s\t1\t%s" % (var[0], var[2]) #fea 1 idx
    elif (var[-1] == '3'): # fea sign 3   # fea  sign1_sign2_sign3 3
        arr = var[1].split("_")
        for fea in arr:
            print "%s\t2\t%s" % (var[0], fea) #fea 2 sign

