#! /usr/bin/python
#encoding:utf-8

import sys
import os

if len(sys.argv) != 2 :
	print "%s model.transposed < model" % (sys.argv[0])
	exit()

prob_col = open(sys.argv[1]+".col", "w")
prob_row = open(sys.argv[1]+".row", "w")
prob_idx = open(sys.argv[1]+".idx", "w")


col_dict = {} 
row_dict = []

element = 0

for line in sys.stdin :
	(labels, features, sign) = line.strip().split("\t")

	(show, click) = labels.split(":")
	(show, click) = (int(float(show)), int(click))

	(feanum, features) = features.split(":")
	fealist = features.split("_")
	
	neg = show - click
	if (show < click) :
		neg = 0
	if (neg == 0 and click == 0) :
		print "error labels"
		exit()
	
	if neg > 0 :
		row_dict.append([-1, neg])
		idx = len(row_dict) - 1
		element += len(fealist)
		for sfea in fealist:
			fea = int(sfea)
			if fea not in col_dict :
				col_dict[fea] = []
			col_dict[fea].append(str(idx))
	if click > 0 :
		row_dict.append([1, click])
		idx = len(row_dict) - 1
		element += len(fealist)
		for sfea in fealist:
			fea = int(sfea)
			if fea not in col_dict :
				col_dict[fea] = []
			col_dict[fea].append(str(idx))

for idx, item in enumerate(row_dict):
	print >>prob_row, "%d %d %d" % (idx, item[0], item[1])

for key in range(len(col_dict)):
	print >>prob_col, "%d %s" % (key + 1, "_".join(col_dict[key + 1]))

print >>prob_idx, "#rows #cols #element"
print >>prob_idx, "%d %d %d" % (len(row_dict), len(col_dict), element)
