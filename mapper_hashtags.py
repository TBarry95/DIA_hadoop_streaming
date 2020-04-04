#!/usr/bin/python

# DES: Mapper script which reads in a list of tweets from 2 datasets, and applies a sentiment score for each tweet.
#      Returns key value pair of date, source and sentiment score.
# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
import sys
from textblob import TextBlob
import csv
import re

for line in csv.reader(sys.stdin): # line = row of data points
    if len(line) >= 14:
        key = line[1] #key = date
        source = line[2]
        hashtags = line[15]
        hashtags = [re.sub("\s+", ",", hashtags.strip())]
        print(('%s,%s,%s') % (key, source, hashtags))
    else: # none here: trump tweets have no hashtags
        key = line[6]  # key = date
        processed_txt = line[7]
        blob = TextBlob(processed_txt)
        sentiment = blob.sentiment.polarity
        value = sentiment
        source = "TRUMP"
        print(('%s,%s,%s') % (key, source, value))

