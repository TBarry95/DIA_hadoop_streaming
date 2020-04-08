from hdfs3 import HDFileSystem

hdfs = HDFileSystem(host='localhost', port=8020)

hdfs = HDFileSystem(host='localhost', port=54310)

hdfs=HDFileSystem(host='localhost',port=9000)

import pandas as pd
from hdfs import InsecureClient
import os

# Connecting to Webhdfs by providing hdfs host ip and webhdfs port (50070 by default)

client = InsecureClient('http://localhost:54310')
# for reading  a file
with client.read('/user/hduser/production/account_apr8/part-00000') as reader:
  features = reader.read()









