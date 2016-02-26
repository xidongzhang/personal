#!/usr/bin/python

import sys

last_key = None
num = 0
limit = int(sys.argv[1])
for line in sys.stdin :
    line=line.strip()
    if(line==""):
        continue
    ll=line.split("\t")
    key = ll[0];
    if last_key != key :
        if last_key and num>=limit:
            print "%s\t%s"%(last_key, num)
        last_key = key
        num = 0

    try:
        num += float(ll[1])
    except:
        continue

if last_key and num>=limit:
    print "%s\t%s"%(last_key, num)
