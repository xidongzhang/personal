#coding=utf8
import sys
import os

if __name__ == '__main__':
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        uid=ll[0]
        tid=ll[1]
        date=ll[2]
        action=ll[3]
        print "%s{/c}%s\t1"%(tid,action)





