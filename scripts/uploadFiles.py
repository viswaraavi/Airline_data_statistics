import boto
import boto.s3
import sys
from boto.s3.key import Key
import os
import glob

srcDir = "./extract"

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

bucket_name = "csc591_dic_airline_data"
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY)


bucket = conn.create_bucket(bucket_name,
    location=boto.s3.connection.Location.DEFAULT)

inputFiles = glob.glob(srcDir + '/*.csv')

def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

for inputFile in inputFiles:
    k = Key(bucket)
    k.key = os.path.basename(inputFile)
    print 'Uploading %s to Amazon S3 bucket %s and key %s' % (inputFile, bucket_name, k.key)
    k.set_contents_from_filename(inputFile, cb=percent_cb, num_cb=10)
    print "\n"
