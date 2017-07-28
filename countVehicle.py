#coding=utf-8
from pyspark import SparkContext, SparkConf, SparkContext
import os
import sys

def judgePalte(x1,x2):
    if len(x1) == 7:
        return x1
    elif len(x2)==7:
        return x2
    else:
        return "empty"
def mapToWord(x):
    items = x.split(";")
    keys = {}
    w0 = items[20].replace(" ","")
    w1 = items[21].replace(" ","")
    w = judgePalte(w0,w1)
    if w!="empty":
        if w not in keys:
            keys[w]=1
        else:
            keys[w]+=1
    return keys
def getResult(x):
    new_key = mapToWord(x)
    for i in new_key:
        if new_key[i]<120:

if __name__ == '__main__':
    sc = SparkContext("spark://ITS-Hadoop10:7077")
    lines = sc.textFile()
    tempstr = getResult(lines)