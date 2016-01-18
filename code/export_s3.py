import boto
from pymongo import MongoClient
from subprocess import call
import json

def export_s3(key_name):
    '''
    Run this on EC2 instance where each machine has a set of restaurant data. Export only these review queries from MongoDB to Amazon S3
    '''
    # Export mongdb results to a local json file
    call(["mongoexport", "-d", "yelp", "-c", "businesses", "-q", "{ type: {$ne: 'business'} }", "-o", "../results/%s" %key_name])
    print '\n'
    # Create connection to AWS keys
    with open('../data/aws.json') as f:
        dat = json.load(f)
        access_key = dat['access-key']
        access_secret_key = dat['secret-access-key']

    ## Connect to AWS account with Boto
    conn = boto.connect_s3(access_key, access_secret_key)
    print "Connecting to S3 ..."

    # If bucket does not exist then create one
    bucket_name = 'yelp_data_V2'
    if conn.lookup(bucket_name) is None:
        b = conn.create_bucket(bucket_name, policy='public-read')
        print "Creating new bucket: %s" % bucket_name
    else:
        b = conn.get_bucket(bucket_name)
        print "%s bucket already exists" % bucket_name

    yelp_file_object = b.new_key(key_name) # create new file in bucket
    file_name = '../results/%s' %key_name
    print "Writing %s key file from %s" %(key_name, file_name)
    yelp_file_object.set_contents_from_filename(file_name, policy='public-read')

if __name__ == '__main__':
    export_s3("results.json")
    # mongoexport -d yelp -c businesses -o results/results.json

    # mongoimport --db <database-name> --collection <collection-name> --file filename.json


    mongoexport --db yelp --collection yelp_500_plus --type=csv --out yelp_businesses.csv
