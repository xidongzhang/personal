#! /usr/bin/env python
#coding=utf-8

import sys
import hashlib
import math
import zipimport
import random
from utils import *
importer = zipimport.zipimporter('jieba.mod')
jieba = importer.load_module('jieba')
jieba.set_dictionary('./dict.txt')

# configå
deli = '\t'
stopword_list = [',', '呢','吧','吗','是','的','地','得','从','当','但是','但','个','各','果然','果真','的', '了', '在', '是', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去', '会', '着', '没有', '看', '好', '自己', '这']

#定义函数：对几个中文串分词，并获得其共同项，差别项，以及相似度，覆盖度等
def compare_chinese_str(str1, str2):
    #分词
    str1_list = str1.split("{/c}")

    str2_list = list(jieba.cut(str2))
    str2_list = [val.encode('utf-8') for val in str2_list]
    str2_list = list(set(str2_list))

    #获取公共匹配项
    #matched = list(set(str1_list).intersection(set(str2_list)))
    matched = [val for val in str2_list if val in str1_list and val != " " and val not in stopword_list]
    matched_len = len(matched)
    matched = "{/c}".join(matched)
    
    #获取非公共项
    #unmatched = [val for ((val in str1_list if val not in str2_list) or(val in str2_list if val not in str1_list)）]
    str1_not_in_str2 = [val for val in str1_list if val not in str2_list and val != " " and val not in stopword_list]
    str2_not_in_str1 = [val for val in str2_list if val not in str1_list and val != " " and val not in stopword_list]
    unmatched = str2_not_in_str1
    unmatched_len = len(unmatched)
    unmatched = "{/c}".join(str2_not_in_str1)

    #相似度
    try:
        similarity = float(matched_len) / float(matched_len + unmatched_len)
    except Exception, e:
        similarity = 0.0
    
    similarity = str("%.2f" %similarity)

    #击中部分占总query的比例
    return matched, str(matched_len), unmatched, str(unmatched_len), similarity
def get_sha1(src):
    return hashlib.sha1(src).hexdigest()

if __name__ == '__main__':
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        show=float(ll[1])
        click=float(ll[2])
        ll[1]=str(show+click)
        confDict1 = initDict(sys.argv[1],0,1)
        confList1 = initList(sys.argv[1],0)
      #  print confDict1
      #  print confList1
        length_old_feature = 0
        for key in confDict1:
            if(confDict1[key] == "1" or confDict1[key]=="2"):
                length_old_feature +=1 
      #  print length_old_feature 
      #  print len(ll[3:])
        confDict={}
        index = 3
        if(len(ll[3:])!=length_old_feature):
            continue
        for feature_ in confList1:
            if(confDict1[feature_]=="1" or confDict1[feature_]=="2"):
                confDict[feature_] = ll[index]
                index +=1 
        title_splits = confDict["title_splits"]
        query = confDict["poster_word"]
       # title_splits = ll[35]
       # query = ll[36]
        matched, matched_len, unmatched, unmatched_len, similarity = compare_chinese_str(title_splits, query)
        ll += [matched, matched_len, unmatched, unmatched_len, similarity]
        uid=ll[0]
        if(uid=="a"):
            r=random.randint(1,1000000)
            ll[0]=str(r)
        vStr="\t".join(ll[3:])
        print "%s\t%s\t%s\t%s\t%s\t"%(ll[0],ll[1],ll[2],get_sha1(vStr),vStr)
        '''
        show=ll[42]
        uid=ll[40]
        click=ll[43]
        ll[42]=ll[0]
        ll[0]=show
        ll[43]=ll[1]
        ll[1]=click
        sign=get_sha1("*".join(ll[2:]))
        print "%s\t%s\t%s\t%s\t%s"%(uid,show,click,sign,"\t".join(ll[2:]))
        '''


