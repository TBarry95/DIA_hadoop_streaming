#!/usr/bin/python

# DES: Credit to LucidProgramming: Youtube: https://www.youtube.com/watch?v=wlnx-7cm4Gg&t=223s
#      Using tweepy API, and following tutorial by Lucid Programming, created module for twitter API.
#      Used for gathering tweets, handling twitter authentication, etc,
# BY:  Tiernan Barry, x19141840 - NCI.

# Libraries:
from tweepy.streaming import StreamListener
from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler
from tweepy import Stream
import twitter_credentials
import pandas as pd
import numpy as np
from textblob import TextBlob
import re

# --- TWITTER_CLIENT --- #
class TwitterClientClass(): #twitter_client_class
    def __init__(self, twit_user=None):
        self.authentication = TwitterAuthenticator().auth_twitter_app()
        self.twitter_client = API(self.authentication)
        self.twit_user = twit_user

    def get_twitter_client_api(self):
        return self.twitter_client

    def get_timeline_tweets(self, num_of_tweets):
        tweet_list = []
        for i in Cursor(self.twitter_client.user_timeline, id = self.twit_user).items(num_of_tweets):
            tweet_list.append(i)
        return tweet_list

    def get_timeline_pages(self, num_of_pages):
        page_list = []
        for i in Cursor(self.twitter_client.user_timeline, id = self.twit_user, tweet_mode='extended', wait_on_rate_limit=True).pages(num_of_pages):
            page_list.append(i)
        return page_list

    def get_home_tweets(self, num_of_tweets):
        tweet_list = []
        for i in Cursor(self.twitter_client.home_timeline, id = self.twit_user, tweet_mode='extended').items(num_of_tweets):
            tweet_list.append(i)
        return tweet_list

    def get_hashtag_tweets(self, num_of_tweets, hashtag):
        tweet_list = []
        for i in Cursor(self.twitter_client.search, q = hashtag, lang = "en").pages(num_of_tweets):
            tweet_list.append(i)
            return(tweet_list)

    def get_location_tweets(self, country, num_of_tweets):
        tweet_list = []
        places = self.twitter_client.geo_search(query=country, granularity="country")
        place_id = places[0].id
        tweets = Cursor(self.twitter_client.search, q="place:%s" % place_id, tweet_mode='extended', wait_on_rate_limit=True).items(num_of_tweets)
        for i in tweets:
            tweet_list.append(i)
            # print(i.text + " | " + i.place.name if i.place else "Undefined place")
        return tweet_list


# -- TWITTER_AUTHENTICATOR -- #
class TwitterAuthenticator(): #twitter_auth

    def auth_twitter_app(self):
        authentication = OAuthHandler(twitter_credentials.consumer_api_key, twitter_credentials.consumer_api_secret_key)
        authentication.set_access_token(twitter_credentials.access_token, twitter_credentials.access_secret_token)
        return authentication

# -- TWITTER_STREAMER -- #
class TweetStreamerClass(): #tweet_streamer
    """
    Class for streaming and processing tweets
    """
    def __init__(self):
        self.twitter_authenticator = TwitterAuthenticator()

    def stream_tweets(self, write_to_filepath, hash_tags_list):
        # handles auth and connection to twitter api.
        listener_object = TwitterListenerClass(write_to_filepath)
        authentication = self.twitter_authenticator.auth_twitter_app()
        stream_object = Stream(authentication, listener_object)
        # filters by keywords
        stream_object.filter(track = hash_tags_list)
        #output_tweets = stream_object.filter(track=hash_tags_list)

# -- TWITTER_STREAM_LISTENER -- #
class TwitterListenerClass(StreamListener): #twitter_listener
    """
    Basic class that prints live tweets to stnd out
    """
    def __init__(self, write_to_filepath):
        self.write_to_filepath = write_to_filepath

    def on_data(self, data_input):
        try:
            with open(self.write_to_filepath, "a") as file:
                file.write(data_input)
            return True
        except BaseException as exc:
            print("Exception on Error:", exc)
        return True

    def on_error(self, status_input):
        if status_input == 420:
            return False
        print(status_input)

# -- TWITTER_ANALYSER -- #
class AnalyseTweetsClass(): #analyse_tweets

    def clean_tweet(self, tweet):
        cleaned_tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
        return cleaned_tweet

    def sentiment_analyser(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))
        if analysis.sentiment.polarity > 0:
            return 1
        elif analysis.sentiment.polarity == 0:
            return 0
        elif analysis.sentiment.polarity < 0:
            return -1

    def dataframe_tweets(self, tweets):
        list_ids = np.array([i.id for i in tweets])
        df = pd.DataFrame({"ID": list_ids})
        df["User"] = np.array([i.user for i in tweets])
        df["Tweets"] = np.array([i.text for i in tweets])
        df["Length"] = np.array([len(i.text) for i in tweets])
        df["Date"] = np.array([i.created_at for i in tweets])
        #df["Likes"] = np.array([i.favorite_count for i in tweets])
        #df["Retweets"] = np.array([i.retweet_count for i in tweets])
        return df