#! /usr/bin/env python
#coding=utf-8

import sys
#import ConfigParser
import urllib
import hashlib
import math
from utils import *

# config
deli = '\t'
feature_deli = "&"
combination_deli = "#"
precision = 0.0001
features_dict={}
discretization_dict={}
position_bias_dict={}
colLimit=int(sys.argv[1])

def is_equal_greater(a, b):
    if math.fabs(a-b) < precision:
        return True
    elif a > b:
        return True
    else:
        return False

def is_equal_less(a, b):
    if math.fabs(a-b) < precision:
        return True
    elif a < b:
        return True
    else:
        return False

def is_equal(a, b):
    if math.fabs(a-b) < precision:
        return True
    return False

def get_sha1(src):
	return hashlib.sha1(src).hexdigest()

def get_md5(src):
    md5 = hashlib.md5()
    md5.update(src)
    return md5.hexdigest()

class Config:  
    def __init__(self, path):  
        self.path = path  
        self.cf = ConfigParser.ConfigParser()  
        self.cf.read(self.path)  
    
    def get_string(self, field, key):  
        result = None 
        try:  
            result = self.cf.get(field, key)  
        except:  
            result = None  
        return result

    def get_int(self, field, key):
        result = None
        try:
            result = self.cf.getint(field, key)
        except:
            result = None
        return result

    def get_float(self, field, key):
        result = None
        try:
            result = self.cf.getfloat(field, key)
        except:
            result = None
        return result

    def get_boolean(self, field, key):
        result = None
        try:
            result = self.cf.getboolean(field, key)
        except:
            result = None
        return result
    
    def get_sections(self):
        return self.cf.sections()

def load_discretization_dict(discretization_path):
    discretization = open(discretization_path)
    for line in discretization:
        items = line.strip().split()
        if len(items) != 2 or str.isdigit(items[1]) == False:
            continue
        key = items[0]
        value = int(items[1])
        discretization_dict[key] = value
    discretization.close()

def get_combine_present(feature_name, value_str) :
	#return urllib.quote(feature_name)+feature_deli+urllib.quote(value_str)
    feature_name=feature_name.replace("\t"," ")
    feature_name=feature_name.replace("\n"," ")
    value_str=value_str.replace("\t"," ")
    value_str=value_str.replace("\n"," ")
    return feature_name+feature_deli+value_str

def  get_discretization_id(feature_name, value_str):
    key = urllib.quote(feature_name)+feature_deli+urllib.quote(value_str)
    if discretization_dict.has_key(key):
        return discretization_dict[key]
    else:
        return None

#normal
#twitter_id poster_id poster_word pos show click feature_1 ... feature_n = n+6
#individuation
#user_id twitter_id poster_id poster_word pos show click feature_1 ... feature_n = n+7

#train_data=open('train_data', 'w')

def printResult(fea_dict_emit,signDict):
    for fea in fea_dict_emit:
        cnt=fea_dict_emit[fea]
        #if(cnt<10 and fea.find("##")>0):
        if(cnt<colLimit and fea.find("#")>=0 and fea.find("user_id")>=0):
            continue
        print "%s\t%s\t2" % (fea, cnt)
        if(fea not in signDict):
            continue
        #for sign in signDict[fea]:
        print "%s\t%s\t3" % (fea, "_".join(signDict[fea]))
    
def flushResult(fea_dict_emit):
    for fea in fea_dict_emit:
        cnt=fea_dict_emit[fea]
        print "%s\t%s\t3" % (fea, sign)
        print "%s\t%s\t2" % (fea, fea_dict_emit[fea])


if __name__ == "__main__":
    #config = Config('discretization.config.0305')
    confList = initList(sys.argv[2],0)
    confDict1 = initDict(sys.argv[2],0,1)
#    config = Config(sys.argv[2])
#    features = config.get_sections()[1:]
    features_count = len(confList)
    common_features_count = 0
    feature_start = 4
    #load_discretization_dict('discretization.dict')
    
    index = feature_start
    for feature_name in confList:
        if feature_name.find(combination_deli) == -1:
            common_features_count += 1

    key = ""
    key_bak = ""
    fea_dict_emit = {}
    for line in sys.stdin:
        items = line.strip().split(deli)
        #if len(items) != feature_start+common_features_count or str.isdigit(items[1]) == False:
        if len(items) != feature_start+common_features_count or (type(eval(items[2])) != int and type(eval(items[2])) != float):
            sys.stderr.write('%s\t%d\t%d' % (feature_name, feature_start, common_features_count))
            continue
        key=items[0]
        if(key!=key_bak):
            if(key_bak!=""):
                printResult(fea_dict_emit,signDict)
            fea_dict_emit = {}
            key_bak = key
            signDict = {}
        
        show_nums = items[1]
        #if(float(show_nums)<1):
        #    continue
        click_nums = items[2]
        index = feature_start

        sign = items[3]
        fea_dict = {}
        desc_dict = {}
        while index < len(items):
            feature_name = confList[index-feature_start]
            if confDict1[feature_name] =="2" or feature_name.find(combination_deli) != -1:
                index +=1
                continue
            #wsscwqx
            #wsscwqx
            if (False):
                pass
            else: # not is_comparable
                values = items[index].split('{/c}')
                for value_str in values:
                    if(value_str!="NULL"):
                        feature = get_combine_present(feature_name, value_str)
                        if feature not in fea_dict :
                            fea_dict[feature] = 1
                            if feature_name not in desc_dict :
                                desc_dict[feature_name] = []
                            desc_dict[feature_name].append(value_str)
                        if(feature not in signDict):
                            signDict[feature] = []
                        signDict[feature].append(sign)
                        if(feature not in fea_dict_emit):
                            fea_dict_emit[feature] = 1
                        else:
                            fea_dict_emit[feature] += 1
            index += 1

		# combination feature
        for feature_name in confList:
            if feature_name.find(combination_deli) != -1:
                first_name = feature_name[:feature_name.find(combination_deli)]
                second_name = feature_name[feature_name.find(combination_deli)+len(combination_deli):]
                third_name = None
                if second_name.find(combination_deli) != -1 :
                    third_name = second_name[second_name.find(combination_deli)+len(combination_deli):]
                    second_name = second_name[:second_name.find(combination_deli)]
                   
                if first_name in desc_dict and second_name in desc_dict:
                    for first_value in desc_dict[first_name] :
                        if(first_value=="NULL"):
                            continue
                        for second_value in desc_dict[second_name] :
                            if(second_value=="NULL"):
                                continue
                            if third_name != None and third_name in desc_dict:
                                for third_value in desc_dict[third_name] :
                                    if(third_value=="NULL"):
                                        continue
                                    #value = str(first_value) + combination_deli + str(second_value) + combination_deli + str(third_value)
                                    #feature = get_combine_present(feature_name, value)
                                    feature="".join([first_name,feature_deli,first_value,combination_deli,second_name,feature_deli,second_value,
                                        combination_deli,third_name,feature_deli,third_value])
                                    if feature not in fea_dict :
                                        fea_dict[feature] = 1
                                    if(feature not in signDict):
                                        signDict[feature] = []
                                    signDict[feature].append(sign)
                                    if(feature not in fea_dict_emit):
                                        fea_dict_emit[feature] = 1
                                    else:
                                        fea_dict_emit[feature] += 1
                            else : 
                                #value = str(first_value) + combination_deli + str(second_value)
                                feature="".join([first_name,feature_deli,first_value,combination_deli,second_name,feature_deli,second_value])
                                #feature = get_combine_present(feature_name, value)
                                if feature not in fea_dict :
                                    fea_dict[feature] = 1
                                if(feature not in signDict):
                                    signDict[feature] = []
                                signDict[feature].append(sign)
                                if(feature not in fea_dict_emit):
                                    fea_dict_emit[feature] = 1
                                else:
                                    fea_dict_emit[feature] += 1

                else:
                    pass
                    #sys.stderr.write('wrong combination feature: %s\n' % (feature_name))
                    
        #output three type files
        #if(show_nums>3):
        print "%s\t%s:%s\t1" % (sign, show_nums, click_nums)
        #for fea in fea_dict_emit :
            #print "%s\t%s\t3" % (fea, sign)
    if(key_bak!=""):
        printResult(fea_dict_emit,signDict)


#train_data.close()

