#! /usr/bin/env python
#coding=utf-8

import sys
import random


last_key = None
sum_show = 0
sum_click = 0
key = None
last_value = None

def getSample():
    a=random.randint(1,10)
    return a

def checkRand(num):
    r=random.randint(1,100)
    if(r<=num):
        return True
    else:
        return False

if __name__ == "__main__":
    limit = int(sys.argv[1])
    rand = int(sys.argv[2])
    for line in sys.stdin:
        var = line.strip().split("\t")
        (key, show_click) = (var[0], var[1])
        #print "%s\t%s\t%s\t%s" % (key, show, click, "\t".join(var[3:]))

        pair = show_click.split(":")
        (show, click) = (float(pair[0]), float(pair[1]))

        if last_key != key :
            if last_key and sum_show >= limit:
                #sampleSign=getSample()
                #if(sampleSign==10):
                #    print "%s:%s\t%s#B" % (str(sum_show), str(sum_click), last_key)
                #else:
                if(sum_show>0 and checkRand(rand)):
                    print "%s:%s\t%s" % (str(sum_show), str(sum_click), last_key)
                sum_show = 0
                sum_click = 0

            last_key = key

        sum_show += show
        sum_click += click
    
    if last_key and sum_show >= limit:
        #sampleSign=getSample()
        #if(sampleSign==10):
        #    print "%s:%s\t%s#B" % (sum_show, sum_click, last_key)
        #else:
        if(sum_show>0 and checkRand(rand)):
            print "%s:%s\t%s" % (sum_show, sum_click, last_key)



