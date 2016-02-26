#coding=utf8
import sys
import os
import hashlib
from utils import *
            
def get_sha1(src):
    return hashlib.sha1(src).hexdigest()

if __name__ == '__main__':
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        show=ll[42]
        uid=ll[40]
        click=ll[43]
        ll[42]=ll[0]
        ll[0]=show
        ll[43]=ll[1]
        ll[1]=click
        sign=get_sha1("*".join(ll[2:]))
        print "%s\t%s\t%s\t%s\t%s"%(uid,show,click,sign,"\t".join(ll[2:]))



