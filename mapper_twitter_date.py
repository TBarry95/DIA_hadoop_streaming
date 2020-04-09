#!/usr/bin/python

# SCRIPT 4: Mapper script used for gathering sentiment analysis by date. Reducer is reducer_stats_sent_date.py.

# DES: Mapper script which reads in processed tweets from standard input, and applies a sentiment score for each tweet.
#      Using the date as the key, mapper returns the date, source, fav_count, rt_count, followers, login_device, sentiment from each tweet.
#      Writes out using standard output.
# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
import sys
from textblob import TextBlob
import csv

# 2. Installations (if needed):
# pip install textblob

for line in csv.reader(sys.stdin): # line = row of data points, uses csv reader to split data.
    if len(line) >= 7:
        date = line[1]
        fav_count = line[4]
        rt_count = line[5]
        followers = line[6]
        login_device = line[3]
        processed_txt = line[7]
        blob = TextBlob(processed_txt)
        sentiment = blob.sentiment.polarity
        print(('%s,%s,%s,%s,%s,%s,%s') % (date, "MEDIA", fav_count, rt_count, followers, login_device, sentiment))
    else:
        continue
