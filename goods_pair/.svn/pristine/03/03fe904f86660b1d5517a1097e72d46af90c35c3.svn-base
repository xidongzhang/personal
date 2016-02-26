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
        if ll[0] == '0' or ll[1] == '' or ll[0] == '':
            continue
            
        print "%s\t%s" % (ll[0], ll[1])
