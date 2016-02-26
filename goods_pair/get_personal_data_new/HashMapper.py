#! /usr/bin/env python
#coding=utf-8

import sys
import ConfigParser
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

if __name__ == "__main__":
    config = Config('discretization.config')
    features = config.get_sections()[1:]
    features_count = len(features)
    common_features_count = 0
    feature_start = 4
    #load_discretization_dict('discretization.dict')
    
    index = feature_start
    for feature_name in features:
        feature_config = {}
        is_comparable = config.get_boolean(feature_name, 'is_comparable')
        max_value = config.get_int(feature_name, 'max_value')
        min_value = config.get_int(feature_name, 'min_value')
        min_unit = config.get_string(feature_name, 'min_unit')
        is_multi = config.get_boolean(feature_name, 'is_multi')
        section_count = config.get_int(feature_name, 'section_count') 
        is_emit = config.get_int(feature_name, 'is_emit') 
        
        if is_comparable == None or max_value == None or min_value == None or section_count == None:
            sys.stderr.write("config error! exit!\n")
            exit(1)
         
        feature_config['name'] = feature_name
        feature_config['is_comparable'] = is_comparable
        feature_config['max_value'] = max_value
        feature_config['min_value'] = min_value
        feature_config['min_unit'] = min_unit
        feature_config['is_multi'] = is_multi
        feature_config['section_count'] = section_count
        feature_config['is_emit'] = is_emit 
        features_dict[index] = feature_config
        index += 1

        if feature_config['name'].find(combination_deli) == -1:
            common_features_count += 1

    key = ""
    key_bak = ""
    fea_dict_emit = {}
    for line in sys.stdin:
        items = line.strip().split(deli)
        if len(items) != feature_start+common_features_count or (type(eval(items[2])) != int and type(eval(items[2])) != float):
            sys.stderr.write('%s\t%d\t%d' % (feature_name, feature_start, common_features_count))
            continue
        key=items[0]
        fea_dict_emit = {}
        show_nums = items[1]
        click_nums = items[2]
        index = feature_start

        sign = items[3]
        fea_dict = {}
        desc_dict = {}
        while index < len(items):
            feature_name = features_dict[index]['name']
            if feature_name.find(combination_deli) != -1:
                continue
            is_comparable = features_dict[index]['is_comparable']
            is_multi = features_dict[index]['is_multi']
            is_emit = features_dict[index]['is_emit']
            min_unit = str(features_dict[index]['min_unit']).count('0')
            if is_comparable == True:
                section_count = features_dict[index]['section_count']
                max_value = float(features_dict[index]['max_value'])
                if section_count <= 0:
                    sys.stderr.write("comparable feature[%s] with wrong section_count[%d]\n" % (feature_name, section_count))
                    index += 1
                if is_multi == True:
                    values = items[index].split('{/c}')
                    for value_str in values:
                        desc_id = 'DFT'
                        if value_str != 'NULL' and value_str != 'None':
                            if min_unit == 0:
                                if is_equal_greater(float(value_str), float(features_dict[index]['max_value'])):
                                    value = int(float(features_dict[index]['max_value']))
                                elif is_equal_less(float(value_str), float(features_dict[index]['min_value'])):
                                    value = int(float(features_dict[index]['min_value']))
                                else:
                                    value = int(float(value_str))
                            else:
                                if is_equal_greater(float(value_str), float(features_dict[index]['max_value'])):
                                    value = round(float(features_dict[index]['max_value']), min_unit)
                                elif is_equal_less(float(value_str), float(features_dict[index]['min_value'])):
                                    value = round(float(features_dict[index]['min_value']), min_unit)
                                else:
                                    value = round(float(value_str), min_unit)
                                #value = int(value / min_unit)
                            desc_id = str(int(value / (max_value / float(section_count))))

                        feature = get_combine_present(feature_name, desc_id)
                        if (feature not in fea_dict) :
                            fea_dict[feature] = 1
                            if feature_name not in desc_dict :
                                desc_dict[feature_name] = []
                            desc_dict[feature_name].append(desc_id)
                        if is_emit == 1 :
                            if(feature not in fea_dict_emit):
                                fea_dict_emit[feature] = 1
                            else:
                                fea_dict_emit[feature] += 1

                else: # not multi
                    value_str = items[index]
                    desc_id = 'DFT'
                    if value_str != 'NULL' and value_str != 'None':
                        if min_unit == 0:
                            if is_equal_greater(float(value_str), float(features_dict[index]['max_value'])):
                                value = int(float(features_dict[index]['max_value']))
                            elif is_equal_less(float(value_str), float(features_dict[index]['min_value'])):
                                value = int(float(features_dict[index]['min_value']))
                            else:
                                value = int(float(value_str))
                        else:
                            if is_equal_greater(float(value_str), float(features_dict[index]['max_value'])):
                                value = round(float(features_dict[index]['max_value']), min_unit)
                            elif is_equal_less(float(value_str), float(features_dict[index]['min_value'])):
                                value = round(float(features_dict[index]['min_value']), min_unit)
                            else:
                                value = round(float(value_str), min_unit)
                            #value = int(value / min_unit)

                        desc_id = str(int(value / (max_value / float(section_count))))

                    feature = get_combine_present(feature_name, str(desc_id))

                    if feature not in fea_dict :
                        fea_dict[feature] = 1
                        if feature_name not in desc_dict :
                            desc_dict[feature_name] = []
                        desc_dict[feature_name].append(desc_id)
                    if is_emit == 1 :
                        if(feature not in fea_dict_emit):
                            fea_dict_emit[feature] = 1
                        else:
                            fea_dict_emit[feature] += 1

            else: # not is_comparable
                if is_multi == True:
                    values = items[index].split('{/c}')
                    for value_str in values:
                        if(value_str!="NULL"):
                            feature = get_combine_present(feature_name, value_str)
                            if feature not in fea_dict :
                                fea_dict[feature] = 1
                                if feature_name not in desc_dict :
                                    desc_dict[feature_name] = []
                                desc_dict[feature_name].append(value_str)
                            if is_emit == 1 :
                                if(feature not in fea_dict_emit):
                                    fea_dict_emit[feature] = 1
                                else:
                                    fea_dict_emit[feature] += 1
                else:
                    value_str = items[index]
                    if(value_str!="NULL"):
                        feature = get_combine_present(feature_name, value_str)
                        if feature not in fea_dict :
                            fea_dict[feature] = 1
                            if feature_name not in desc_dict :
                                desc_dict[feature_name] = []
                            desc_dict[feature_name].append(value_str)
                        if is_emit == 1 :
                            if(feature not in fea_dict_emit):
                                fea_dict_emit[feature] = 1
                            else:
                                fea_dict_emit[feature] += 1
            index += 1

		# combination feature
        for index, feature_info in features_dict.items():
            feature_name = feature_info['name']
            if feature_name.find(combination_deli) != -1:
                is_emit = feature_info['is_emit']
                if is_emit != 1 :
                    continue
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
                                if(feature not in fea_dict_emit):
                                    fea_dict_emit[feature] = 1
                                else:
                                    fea_dict_emit[feature] += 1

                else:
                    pass
                    #sys.stderr.write('wrong combination feature: %s\n' % (feature_name))
        #mod=134217728  # pow(2,27)
        mod=67108864  # pow(2,26)
        hashDict={}    
        for fea in fea_dict_emit:
            h=getHash(fea)
            fid=h%mod+2
            hashDict[fid]=1
        fList=[]
        for fid in hashDict:
            fList.append(fid)
        fList.sort(reverse=False)
        fList.insert(0,1)
        print "%s:%s %s:%s"%(float(show_nums)+float(click_nums),click_nums,len(fList),"_".join(map(str,fList)))




