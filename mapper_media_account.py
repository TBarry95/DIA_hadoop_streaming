#!/usr/bin/python

# DES: Mapper script which reads in a list of tweets, and applies a sentiment score for each tweet.
#      Returns key value pair of sentiment score and
# BY:  Tiernan Barry, x19141840 - NCI.

# 1. Libraries:
import sys
from textblob import TextBlob
import csv
import pandas as pd

# 2. Data lists: In order to order by column, need make DF first and then print as CSV
dt = []
source = []
fav_count = []
rt_count = []
followers = []
processed_txt = []
sentiment = []

# 2. Reads stnd in and writes to above lists
for line in csv.reader(sys.stdin): # line = row of data points
    dt.append(line[1])
    source.append(line[2])
    fav_count.append(line[7])
    rt_count.append(line[8])
    followers.append(line[9])
    processed_txt.append(line[14])
    blob = TextBlob(line[14])
    sentiment.append(blob.sentiment.polarity)

df = pd.DataFrame()
df['date'] = dt
df['source'] = source
df['fav_count'] = fav_count
df['rt_count'] = rt_count
df['followers'] = followers
df['sentiment'] = sentiment

# sort DF by media account:
df = df.sort_values(by=['source'])
# sort DF by media account:
print(df.to_csv(sep=',', index=False, header=None))