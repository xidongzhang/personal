#coding=utf8
import sys
import os
import time

def proccessColor(color):
    arr = color.split("__")
    if(len(arr) < 2):
        return "-"
    colorStr = arr[1]
    colorStr = colorStr.replace("\t"," ")
    colorStr = colorStr.replace("\n"," ")
    p = colorStr.find(" ")
    if(p >= 0):
        colorStr = colorStr[0:p]
        return colorStr
    p = colorStr.find("(")
    if(p >= 0):
        colorStr = colorStr[0:p]
        return colorStr
    p = colorStr.find("【")
    if(p >= 0):
        colorStr = colorStr[0:p]
        return colorStr
    p = colorStr.find("[")
    if(p >= 0):
        colorStr = colorStr[0:p]
        return colorStr
    return colorStr
    


if __name__ == '__main__':
    input_file = os.environ['map_input_file']
    arr = input_file.split("/")
    inputPath = arr[len(arr)-2]
    wDict = {"comp_from_action":1,"comp_from_luna":100,"comp_from_order":5
            ,"subti_from_action":1,"subti_from_cart":1,"subti_from_like":1}
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        sign, id, weight, tid1, tid2, ctime1, price1, color1, size1, ctime2, price2, color2, size2 = line.split("\t")
        pairType = inputPath[0:inputPath.find("_")]
        w = weight
        color1 = proccessColor(color1)
        color2 = proccessColor(color2)
        if(inputPath in wDict and w == "1"):
            w = wDict[inputPath]
        buf = [tid1, tid2, pairType, color1, color2]
        print "%s\t%s"%("{/c}".join(buf),w)

        
        

        
         













