#coding=utf8
import sys
import os
#from nlpir import *

def checkUsed(start,length,posUsedDict):
    for i in range(start,start+length):
        if(i in posUsedDict):
            return False
    return True
'''
def segWords(line):
    rList=[]
    for t in Seg(line):
        rList.append(t[0])
    return rList
'''

def wordsJoin(wordList,metaDict,mustHit=False):
    resultArr=[]
    posUsedDict={}
    for length in range(4,0,-1):
        if(len(wordList)<length):
            continue
        for i in range(len(wordList)-length+1):
            sign=checkUsed(i,length,posUsedDict)
            if(not sign):
                continue
            if(length==1):
                wordStr=wordList[i]
                if(wordStr in metaDict or not mustHit):
                    resultArr.append(wordList[i])
                    posUsedDict[i]=1
            else:
                wordStr="".join(wordList[i:i+length])
                if(wordStr in metaDict):
                    resultArr.append(wordStr)
                    for j in range(i,i+length):
                        posUsedDict[j]=1
    return resultArr


if __name__ == '__main__':
    pass
         













