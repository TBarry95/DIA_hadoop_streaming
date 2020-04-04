#!/usr/bin/python

# DES: Reducer script to find summary statistics for sentiment analysis scores
#      finds mean, median, max, min, standard deviation for each date.

# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
from operator import itemgetter
import sys
import re
from sortedcontainers import SortedList
import statistics as stats
import csv

# 2. reduce key,values by date and find median score per date:
last_date_key = None
count_tweet_date = 0
hashtag_list = []


#print("DATE, MEAN, STND_DEV, MEDIAN, MIN, MAX, COUNT")

for key_value in csv.reader(sys.stdin):
    this_date_key = key_value[0]
    source = key_value[1]
    hashtag = key_value[2]

    if last_date_key == this_date_key:
        count_tweet_date += 1
        hashtag_list.append(hashtag)

    else:
        if last_date_key:
            print(('%s,%s,%s') % (last_date_key, count_tweet_date, hashtag_list))  # 2
        last_date_key = this_date_key
        count_per_date = 1

# -- Output summary stats:
if last_date_key == this_date_key:
    print(('%s,%s,%s') % (last_date_key, count_tweet_date, hashtag_list))  # 2






