# coding=utf-8
import socket
import sys
import boto3

client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
port = 9090
if len(sys.argv) == 2 :
    port = int(sys.argv[1])
host = 'localhost'
client.connect((host,port))
data = client.recv(2048) 
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
    data = client.recv(2048) 


    if data.decode().strip() == 'exit':
        client.close() 
        break
    elif data.decode().strip() == 'Register successfully.':
        # create the bucket
        bucket = client.recv(2048).decode()
        print(bucket)
        s3.create_bucket(Bucket = bucket)

    elif data.decode()[:8] == 'Welcome,':
        bucket = client.recv(2048).decode()
        current_bucket = bucket
        print("current bucket is :",current_bucket)
    
    if data.decode().strip() != '':
        print(data.decode(), end = '')


