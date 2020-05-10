# coding=utf-8
import socket
import sys
import boto3
import os

client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
port = 9090
if len(sys.argv) == 2 :
    port = int(sys.argv[1])
host = 'localhost'
client.connect((host,port))

data = client.recv(4096) 
print(data.decode(), end = '')
current_bucket = ''

s3 = boto3.resource('s3')
while True:
    #prompt
    print('% ', end = '')
    input_str = input()
    if input_str.strip() == '':
        continue
    client.send(input_str.encode())
    data = client.recv(4096).decode()


    if data.strip() == 'exit':
        client.close() 
        break

    elif data.strip() == 'Register successfully.':
        # create the bucket
        bucket = client.recv(4096).decode()
        print(bucket)
        s3.create_bucket(Bucket = bucket)


    elif data[:8] == 'Welcome,':
        bucket = client.recv(4096).decode()
        current_bucket = bucket
        print("current bucket is :",current_bucket)


    elif data[:5] == 'Bye,':
        current_bucket = ''


    
    elif data.strip() == 'Create post successfully.':
        # file name and its dir
        object_name = client.recv(4096).decode()
        dest_dir = './tmp/' + object_name

        target_bucket = s3.Bucket(current_bucket)
        target_bucket.upload_file(dest_dir, object_name)

    
    elif data.strip() == 'Read_post':
        meta_data = client.recv(4096).decode()
        print(meta_data, end = "")
        

        postdata = client.recv(4096).decode()
        author_bucket = postdata.split()[1]
        object_name = postdata.split()[0]
        print('author_bucket', author_bucket)

        target_bucket = s3.Bucket(author_bucket)
        target_object = target_bucket.Object(object_name)
        object_content = target_object.get()['Body'].read().decode()
        print(object_content)
        # print('--\n\r')
    
    elif data.strip() == 'Comment successfully.':
        postdata = client.recv(4096).decode()
        print('1: ', postdata)
        author_bucket = postdata.split()[1]
        object_name = postdata.split()[0]
        comment = client.recv(4096).decode()
        print('2: ', comment)
        target_bucket = s3.Bucket(author_bucket)
        target_object = target_bucket.Object(object_name)
        object_content = target_object.get()['Body'].read().decode()
        # print('bucket: ', author_bucket, 'oid', object_name, 'comment', comment)
        print('3: ', object_content)
        new_content = object_content + comment
        dest_dir = './tmp/' + object_name
        with open(os.path.join('C:\\Users\\Chiang Chieh Ming\\Desktop\\Network-Programming\\hw3\\tmp',object_name), "w") as file:
                file.write(new_content)
                file.close()
        target_bucket.upload_file(dest_dir, object_name)


        

    print(data, end = '')





    # for i in range(int(data[-1:])):
    #     tmp = client.recv(4096).decode()
    #     print(tmp, end = '')


