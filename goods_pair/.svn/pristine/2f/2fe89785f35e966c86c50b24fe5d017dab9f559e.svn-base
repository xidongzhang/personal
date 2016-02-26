#coding=utf8
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        line = line.strip()
        if line == "":
            continue
        ll = line.split("\001")
        if len(ll) < 2:
            continue
            
        print "%s\t%s" % (ll[1], ll[0])
