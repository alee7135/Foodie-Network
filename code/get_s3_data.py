import boto
from pymongo import MongoClient
from subprocess import call
import json
import os
import pandas as pd

'''
Gather up all the data from AWS and consolidate down to single results file
'''

# Initiate MongoDB Database & Collection
client = MongoClient()
db = client['yelp']
coll = db['yelp_500_plus_V2']

with open('../data/aws.json') as f:
    dat = json.load(f)
    access_key = dat['access-key']
    access_secret_key = dat['secret-access-key']

## Connect to AWS account with Boto
conn = boto.connect_s3(access_key, access_secret_key)
print "Connecting to S3 ..."

bucket_name = 'yelp_data'
b = conn.get_bucket(bucket_name)

## Get each file from S3 and write it to local directory
FILE_PATH = "../results/"
for key in b.get_all_keys():
    keyString = str(key)
    # check if file exists locally, if not: download it
    if not os.path.exists(LOCAL_PATH+keyString):
        print "Writing %s contents to file ..." %(keyString)
        key.get_contents_to_filename(FILE_PATH + keyString)

# Loop through each file and consolidate all json files to single master file
master_file = []
for fle in os.listdir('../results/'):
    if fle.startswith("<Key"):
        print "Working on file %s" %fle
        f = open("../results/%s" %fle, 'r+')
        for line in f:
            master_file.append(line.strip('\n'))
        f.close()
# Loop through and convert to list of json files so we can operate with Pandas
master_file_final = []
for st in master_file:
    j = json.loads(st)
    j.pop("_id", None)
    master_file_final.append(j)

# Use Pandas to delete duplicates
df = pd.DataFrame(master_file_final)
df_user = df[df['type']=='user']
df_business = df[df['type']=='business']
df_review = df[df['type']=='review']
df_user = df_user.drop_duplicates(['type', 'user_id'])
df_business = df_business.drop_duplicates(['type', 'id'])
df_review = df_review.drop_duplicates(['type','user_id', 'business_id'])
df = pd.concat([df_business, df_review, df_user], axis=0)

## Insert all unique records into a new Mongo Collection
coll.remove()
df.apply(lambda x: coll.insert(x.dropna().to_dict()), axis=1)
# with open("../results/test", 'w') as f:
#     json.dump(df.dropna(axis=1).to_dict('record'), f)
# coll.insert_many(df.dropna(axis=1).to_dict('record'))
print coll.count()

# call(["mongoimport", "--db", "yelp", "--collection", 'yelp_500_plus', "--file", "../results/%s" %fle])
