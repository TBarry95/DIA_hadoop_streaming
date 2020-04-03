#!/usr/bin/python

# DES: Reducer script to find summary statistics for sentiment analysis scores
#      finds mean, median, max, min, standard deviation for each date.

# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
from operator import itemgetter
import sys
from sortedcontainers import SortedList
import statistics as stats

# 2. reduce key,values by date and find median score per date:
last_date_key = None
sent_list_sort = SortedList()
count_per_date = 0
aggregate_sentiment = 0

print("DATE", "MEAN", "STND_DEV", "MEDIAN", "MIN", "MAX", "COUNT")
for sentiment in sys.stdin:
    print(sentiment[0])