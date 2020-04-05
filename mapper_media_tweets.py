#!/usr/bin/python

# DES: Mapper script which reads in a list of tweets from 2 datasets, and applies a sentiment score for each tweet.
#      Returns key value pair of date, source and sentiment score.
# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
import sys
from textblob import TextBlob
import csv

for line in csv.reader(sys.stdin): # line = row of data points
    if len(line) >= 14:
        date = line[1]
        fav_count = line[7]
        rt_count = line[8]
        followers = line[9]
        login_device = line[6]
        processed_txt = line[14]
        blob = TextBlob(processed_txt)
        sentiment = blob.sentiment.polarity
        print(('%s,%s,%s,%s,%s,%s,%s') % (date, "MEDIA", fav_count, rt_count, followers, login_device, sentiment))
    else:
        continue


'''key = line[6]  # key = date
        processed_txt = line[7]
        blob = TextBlob(processed_txt)
        sentiment = blob.sentiment.polarity
        value = sentiment
        print(('%s,%s,%s') % (key, source, value))
'''
