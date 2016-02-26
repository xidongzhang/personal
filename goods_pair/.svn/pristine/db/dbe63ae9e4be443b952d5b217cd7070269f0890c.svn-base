#! /usr/bin/env python
#coding=utf-8

import sys

last_key = None
sum_show = 0
sum_click = 0
key = None
last_value = None

if __name__ == "__main__":
    for line in sys.stdin:
        var = line.strip().split("\t")
        (key, show, click) = (var[0], var[1], var[2])
        #print "%s\t%s\t%s\t%s" % (key, show, click, "\t".join(var[3:]))

        if last_key != key :
            if last_key :
                print "%s\t%d\t%s\t%s" % (str(sum_show), sum_click, last_key, last_value)
                sum_show = 0
                sum_click = 0

            last_key = key  
            last_value = "\t".join(var[3:])

        sum_show += float(show)
        sum_click += int(click)
    
    if last_key :
        print "%s\t%d\t%s\t%s" % (sum_show, sum_click, last_key, last_value)

