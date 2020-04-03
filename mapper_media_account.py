#!/usr/bin/python

# DES: Mapper script which reads in a list of tweets, and applies a sentiment score for each tweet.
#      Returns key value pair of sentiment score and the date of tweet.
# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
import sys
from textblob import TextBlob
import csv

for line in csv.reader(sys.stdin): # line = row of data points
    key = [line[1],line[2]]  # key = date
    processed_txt = line[14]
    blob = TextBlob(processed_txt)
    sentiment = blob.sentiment.polarity
    #rest_of_data = [line[0],line[2:14], value] #,line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12],line[13],line[14]]
    #value = [line[0],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12],line[13],line[14], sentiment]
    value = sentiment
    print(('%s\t%s') % (key, value))
