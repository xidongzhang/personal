#!/usr/bin/python

import sys

for line in sys.stdin :
    var = line.strip().split("\t")
    if(not var[1].startswith("1:")):
        print "%s\t%s" % (var[1], var[0])
