#! /usr/bin/env python
#coding=utf-8

import sys

last_fea = None

if __name__ == "__main__":
    for line in sys.stdin:
        var = line.strip().split("\t")
        type = var[-1]
        if type == '2' :
            fea = var[0]
            
            if last_fea != fea :
                if last_fea :
                    print "%s\t2"
                last_fea = fea
        else :
            print line.strip()

    if last_fea :
        print "%s\t2"
