#! /usr/bin/python
#encoding:utf-8

import os
import sys

fout = open("rule_log", "w")

def initRecallRuleDict(modelWeightDict, modelRecallDict, modelPath):
    avgWeight = 0.0
    sumFreq = 0.0
    sumWeight = 0.0
    avgFreqDict = {}
    lineBuf = []
    fin = open(modelPath)
    for line in fin:
        line=line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        lineBuf.append(ll)
        fea = ll[0]
        weight = float(ll[1])
        freq = int(ll[2])
        sumFreq += freq
        sumWeight += weight*freq
        modelWeightDict[fea] = weight

        nArr = fea.split("&")
        feaName = nArr[0]
        if(feaName.find("cata") < 0 and feaName.find("shop_id") < 0):
            continue
        feaValue = nArr[1]
        vArr = feaValue.split("#")
        value1 = vArr[0]
        value2 = vArr[1]
        if(feaName not in avgFreqDict):
            avgFreqDict[feaName] = {}
        if(value1 not in avgFreqDict[feaName]):
            avgFreqDict[feaName][value1] = [0.0, 0.0]  #sum,num,avg
        avgFreqDict[feaName][value1][0] += freq
        avgFreqDict[feaName][value1][1] += 1
    fin.close()
    avgWeight = sumWeight/sumFreq
    fout.write("avg weight = %s\n"%(avgWeight))
    fout.flush()
    for feaName in avgFreqDict:
        for value1 in avgFreqDict[feaName]:
            #fout.write("%s\t%s\n"%(avgFreqDict[feaName][value1][0], avgFreqDict[feaName][value1][1]))
            #fout.flush()
            num = avgFreqDict[feaName][value1][1]
            if(num <= 0):
                continue
            sum = avgFreqDict[feaName][value1][0]
            avgFreqDict[feaName][value1] = sum/num
            #print "--------%s\t%s\t%s"%(feaName, value1, avgFreqDict[feaName][value1])
    
    recallDict = {}
    for ll in lineBuf:
        fea = ll[0]
        weight = float(ll[1])
        freq = int(ll[2])

        nArr = fea.split("&")
        feaName = nArr[0]
        if(feaName == "cata"):
            continue
        if(feaName.find("cata") < 0 and feaName.find("shop_id") < 0):
            continue
        feaValue = nArr[1]
        vArr = feaValue.split("#")
        value1 = vArr[0]
        value2 = vArr[1]
        #fout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n"%(fea, feaName, value1, freq, avgFreqDict[feaName][value1], weight, avgWeight))
        #fout.flush()
        if(freq > avgFreqDict[feaName][value1] and weight > avgWeight):
        #if(weight > avgWeight):
            key = "%s&%s"%(feaName, value1)
            if(key not in recallDict):
                recallDict[key] = []
            recallDict[key].append((value2, freq, weight))
    lineBuf = []
    for key in recallDict:
        recallDict[key].sort(cmp=lambda x,y:cmp(x[1],y[1]),reverse=True)
        recallDict[key] = recallDict[key][0:7]
        modelRecallDict[key] = []
        for tpl in recallDict[key]:
            modelRecallDict[key].append((tpl[0], tpl[2]))
    #modelRecallDict = recallDict
    '''
    for key in modelRecallDict:
        for tpl in modelRecallDict[key]:
            fout.write("%s\t%s\t%s\n"%(key, tpl[0], tpl[1]))
    fout.flush()
    '''
    fout.write("*******model index load finished*******\n")
    fout.flush()


if __name__ == "__main__":
    modelWeightDict = {}
    modelRuleDict = {}
    initRecallRuleDict(modelWeightDict, modelRuleDict, sys.argv[1])
    for key in modelRuleDict:
        for tpl in modelRuleDict[key]:
            print "%s\t%s\t%s"%("\t".join(key.split("&")), tpl[0], tpl[1])
    
    cataPath = sys.argv[2]
    fin = open(cataPath)
    for line in fin:
        line=line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        cata1 = ll[0]
        cata2 = ll[1]
        cosSim = ll[5]
        print "%s\t%s\t%s\t%s"%("cata", cata1, cata2, cosSim)
    fin.close()






