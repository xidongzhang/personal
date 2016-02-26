#!/usr/bin/python

import sys

for line in sys.stdin :
    var = line.strip().split("\t")
    if (var[-1] == '1'): # sign show:click 1
        print "%s\t1\t%s" % (var[0], var[1])
    elif (var[-1] == '4') : # fea sign idx
        idx = var[1]
        arr = var[0].split("_")
        for sign in arr:
            print "%s\t2\t%s" % (sign, idx)
