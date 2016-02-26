#!/usr/bin/python
#coding=utf8
import sys

minLimit = int(sys.argv[1])
def printResult(key_bak, sum, srcList, sign):
    if(sign == "1"):
        return
    fea = key_bak
    #if(sum < minLimit and not fea.startswith("twitter_id")):
    if(sum < minLimit):
        return
    print "%s\t%s\t%s"%(fea, sum, 2)
    if(len(srcList) > 0):
        print "%s\t%s\t%s"%(key, "_".join(srcList), 3)

if __name__ == '__main__':
    key = ""
    key_bak = ""
    sum = 0
    srcList = []
    limitLen = 5000

    for line in sys.stdin:
        if(line.strip() == ''):
            continue
        ll = line.strip().split('\t')
        key = ll[0]
        if(key != key_bak):
            if(key_bak != ""):
                printResult(key_bak, sum, srcList, sign)
            sum = 0
            srcList = []
            key_bak = key

        sign = ll[len(ll)-1]
        if(sign == "2"):
            sum += int(ll[1])
        if(sign == "3"):
            arr = ll[1].split("_")
            srcList.extend(arr)
            if(len(srcList) > limitLen):
                print "%s\t%s\t%s"%(key, "_".join(srcList), 3)
                srcList = []
        if(sign == "1"):
            print line.strip()
            

    if(key_bak != ""):
        printResult(key_bak, sum, srcList, sign)



