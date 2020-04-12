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

# (tweet_id, date, source, login_device, fav_count, rt_count, followers, filtered_processed_text))

for line in csv.reader(sys.stdin): # line = row of data points, uses csv reader to split data.
    if len(line) >= 7:
        date = line[1]
        source = line[2]
        login_device = line[3]
        fav_count = line[4]
        rt_count = line[5]
        followers = line[6]
        processed_txt = line[7]
        blob = TextBlob(processed_txt)
        sentiment = blob.sentiment.polarity
        print(('%s,%s,%s,%s,%s,%s,%s') % (date, "MEDIA", fav_count, rt_count, followers, login_device, sentiment))
        #print(('%s,%s,%s,%s,%s,%s,%s,%s') % (0, date, "MEDIA", fav_count, rt_count, followers, login_device, sentiment))
        #print(('%s,%s,%s,%s,%s,%s,%s,%s') % (1, date, source, fav_count, rt_count, followers, login_device, sentiment))
    else:
        continue
