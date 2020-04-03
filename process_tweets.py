import numpy as np
import pandas as pd
import re
# import missingno as msno
import matplotlib.pyplot as plt
import warnings
import json
import functions_tweet_mapreduce as fns

##########################################################################
# Extract:
##########################################################################

# -- Read in Media tweets:
media_tweets = pd.read_csv("/home/tiernan/PycharmProjects/DIA/twitter_mass_media_data.csv")

# -- Read in Trump tweets:
#trump_tweets = pd.read_csv("/home/tiernan/PycharmProjects/DIA/trump_tweets.csv")

with open("/home/tiernan/PycharmProjects/DIA/trump_tweets.json") as tweets:
    data = json.load(tweets)
trump_tweets = fns.get_json_data_to_df(data)

# format date:
trump_tweets['DATE_TIME'] = fns.tweet_date_format(trump_tweets)
del trump_tweets['DATE']

##########################################################################
# Transform:
##########################################################################

# -- Deal with NA values: Back fill followed by forward fill
msno.matrix(media_tweets, figsize= (50,30))
msno.matrix(trump_tweets, figsize= (50,30))

# -- Make new column for processed name:
media_tweets['PROCESSED_TEXT'] = media_tweets['FULL_TEXT'].map(lambda i: re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(RT)", '', i))
trump_tweets['PROCESSED_TEXT'] = trump_tweets['TEXT'].map(lambda i: re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(RT)", '', str(i)))

# -- Write out test set and full set without headers:
media_tweets[90000:91000].to_csv("/home/tiernan/PycharmProjects/DIA/twitter_media_sample.csv", index= False, header=None)
media_tweets.to_csv("/home/tiernan/PycharmProjects/DIA/twitter_media_prod.csv", index= False, header=None)

# -- Write out test set and full set without headers:
trump_tweets[30000:31000].to_csv("/home/tiernan/PycharmProjects/DIA/trump_sample.csv", index= False, header=None)
trump_tweets.to_csv("/home/tiernan/PycharmProjects/DIA/trump_prod.csv", index= False, header=None)

# -- Check for formatting using word cloud:
#word_cloud = fns.get_wordcloud(media_tweets, "/home/tiernan/PycharmProjects/DIA/twitter_media_wordcloud.png")

