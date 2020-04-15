#!/usr/bin/python

# SCRIPT 7: Mapper script used for gathering sentiment analysis by Twitter account. Reducer is reducer_twitter_account.py

# DES: Mapper script which reads twitter data, and applies a sentiment score for each tweet.
#      Returns key value pair of data + sentiment score per tweet. No data is loss in this process.
#      Key = Account, Value = Data + sentiment
# BY:  Tiernan Barry, x19141840 - NCI.


# 1. Libraries:
import sys
from textblob import TextBlob
import csv

# 2. Data lists: In order to order by column, need make DF first and then print as CSV

# (tweet_id, date, source, login_device, fav_count, rt_count, followers, filtered_processed_text))

for line in csv.reader(sys.stdin): # line = row of data points
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
        print(('%s,%s,%s,%s,%s,%s,%s') % (source, date, fav_count, rt_count, followers, login_device, sentiment)) #
    else:
        continue
