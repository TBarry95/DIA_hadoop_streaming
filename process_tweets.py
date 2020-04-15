# !/usr/bin/python

# SCRIPT 2: Process data for MapReduce job.

# DES: Goal of this script is to transform data sourced from get_twitter_data.py (script 1)
#      so that the data is prepared for sentiment analysis using map reduce (hadoop streaming).
#      Therefore, ensured that the text data is processed correctly, by removing all punctuation
#      Results in 2 additional columns to dataset: PROCESSED_TEXT, PROCESSED_HASHTAG
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
import pandas as pd
import re
import missingno as msno

##########################################################################
# Extract:
##########################################################################

# -- Read in Media tweets:
media_tweets = pd.read_csv("/home/tiernan/PycharmProjects/DIA/twitter_mass_media_data.csv")

##########################################################################
# Transform:
##########################################################################

# -- Check for duplicates:
media_tweets.drop_duplicates()

# -- Check for NA values:
msno.matrix(media_tweets, figsize= (50,30))
# -- NA columns not important.

# -- Make new column for processed name nad hashtags where possible:
media_tweets['PROCESSED_TEXT'] = media_tweets['FULL_TEXT'].map(lambda i: re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(RT)", '', i))
media_tweets['PROCESSED_HASHTAG'] = media_tweets['HASHTAGS'].map(lambda i: re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(RT)|(text)|(indices)|[0-9]+", '', i))

# -- Write out test dataset for testing locally.
media_tweets[100000:102000].to_csv("/home/tiernan/PycharmProjects/DIA/twitter_media_sample.csv", index= False, header=None)

# -- Write out full dataset for running sentiment analysis in HDFS.
media_tweets.to_csv("/home/tiernan/PycharmProjects/DIA/twitter_media_prod.csv", index= False, header=None)

##########################################################################

# -- Now that input data is cleaned for sentimetn analysis in HDFS, send all files to cloud in Script 3: send_files_to_cloud.py
# -- Map job will remove stop words (script 4)