import sqlite3
import sys
import time
import os
import boto3

conn = sqlite3.connect('bbs.db')
c = conn.cursor()
s3 = boto3.resource('s3')

# delete post
cursor = c.execute("SELECT Object_id, Userbucket FROM post " )
for row in cursor:
    object_name = row[0]
    author_bucket = row[1]
    target_bucket = s3.Bucket(author_bucket)
    target_object = target_bucket.Object(object_name)
    target_object.delete()



# delete mail
cursor = c.execute("SELECT Object_id, Userbucket FROM mail" )
for row in cursor:
    object_name = row[0]
    author_bucket = row[1]
    target_bucket = s3.Bucket(author_bucket)
    target_object = target_bucket.Object(object_name)
    target_object.delete()



# delete bucket
cursor = c.execute("SELECT Bucketname FROM user " )
for row in cursor:
    bucket_name = row[0]
    target_bucket = s3.Bucket(bucket_name)
    target_bucket.delete()

conn.commit()
conn.close()
