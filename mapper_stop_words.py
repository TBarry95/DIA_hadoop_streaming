#!/usr/bin/python

# SCRIPT 4: Mapper script used once punctuation is remove from text.
#           This mapper removes all stop words and reduces text data into essential words.
#           the output of this mapper feed the following 2 map-reduce jobs as inputs datasets.
#           No reducer is needed for this mapper.

# DES: Mapper script which reads
# BY:  Tiernan Barry, x19141840 - NCI.

from nltk.corpus import stopwords
import nltk
from nltk.tokenize import word_tokenize
import csv
import sys
nltk.download('stopwords')
nltk.download('punkt')

for line in csv.reader(sys.stdin): # line = row of data points
    if len(line) >= 14:
        tweet_id = line[0] # key = tweet_id
        date = line[1]
        source = line[2]
        login_device = line[6]
        fav_count = line[7]
        rt_count = line[8]
        followers = line[9]
        processed_txt = line[14].lower()
        stopwords_list = stopwords.words('english')
        processed_txt_token = word_tokenize(processed_txt)
        filtered_processed_text = [i for i in processed_txt_token if i not in stopwords_list]
        filtered_processed_text = ' '.join(filtered_processed_text)
        print(('%s,%s,%s,%s,%s,%s,%s,%s') %
              (tweet_id, date, source, login_device, fav_count, rt_count, followers, filtered_processed_text)) #
    else:
        continue
