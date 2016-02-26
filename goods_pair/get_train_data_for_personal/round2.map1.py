#!/usr/bin/python

import sys

for line in sys.stdin :
    var = line.strip().split("\t")
    if (var[-1] == '2'):
        print "%s\t%s" % (var[0], var[1])

