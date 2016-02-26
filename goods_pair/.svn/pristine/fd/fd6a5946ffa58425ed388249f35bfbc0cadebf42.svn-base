import sys
import yaml

if __name__ == '__main__':
    confDict=yaml.load(open('conf.yaml')) 
    confList=[]
    for feaName in confDict["attr"]:
        confList.append("")
    for feaName in confDict["attr"]:
        index=int(confDict["attr"][feaName]["index"])
        confList[index]=feaName
    for feaName in confList:
        print feaName

