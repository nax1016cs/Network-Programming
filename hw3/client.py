# coding=utf-8
import socket
import sys
import boto3
import os
import re
path = 'C:\\Users\\Chiang Chieh Ming\\Desktop\\Network-Programming\\hw3\\tmp\\'
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


def get_bucket_obj(client, postdata):
    # print(postdata)
    author_bucket = postdata.split()[1]
    object_name = postdata.split()[0]
    # print('author_bucket', author_bucket)
    # print('object-name', object_name)

    target_bucket = s3.Bucket(author_bucket)
    target_object = target_bucket.Object(object_name)
    return target_object, object_name, target_bucket


def get_comment(obj_name):
    dest_dir = path + obj_name
    f = open(dest_dir, 'r')
    lines = f.readlines()
    # print(lines)
    content = "".join(lines)
    # print(content)
    last_char_index = [ m.end(0) for m in re.finditer('--', content)]
    # print(last_char_index)
    comment = content[last_char_index[-1]:]
    # print(comment)
    f.close()
    # pass
    return comment





if  __name__ == "__main__":
    # print(get_comment('C:\\Users\\Chiang Chieh Ming\\Desktop\\Network-Programming\\hw3\\tmp\\test1589171440.txt'))

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
            
            target_object, object_name, target_bucket = get_bucket_obj(client,postdata)
            object_content = target_object.get()['Body'].read().decode()
            print(object_content, end = "")
            # print('--\n\r')
        
        elif data.strip() == 'Comment successfully.':

            postdata = client.recv(4096).decode()
            target_object, object_name, target_bucket = get_bucket_obj(client, postdata)
            object_content = target_object.get()['Body'].read().decode()

            #recieve comment
            comment = client.recv(4096).decode()
            dest_dir = './tmp/' + object_name
            with open(os.path.join(path,object_name), "a+") as file:
                    file.write(comment)
                    # file.close()
            target_bucket.upload_file(dest_dir, object_name)

        elif data.strip() == 'Delete successfully.':
            postdata = client.recv(4096).decode()
            target_object, object_name, target_bucket = get_bucket_obj(client, postdata)
            target_object.delete()
            path = path + object_name
            os.remove(path)

        elif data.strip() == 'Update successfully.':
            message = client.recv(4096).decode()
            if message.strip() == '':
                pass
            else:
                target_object, object_name, target_bucket = get_bucket_obj(client, message)
                content = client.recv(4096).decode()
                ### update the content
                comment = get_comment(object_name).strip()
                new_content = '--\n\r' + content + '\n\r--\n\r' + comment + '\n\r'
                dest_dir = './tmp/' + object_name
                
                with open(os.path.join(path,object_name), "w+") as file:
                    file.write(new_content)
                    # file.close()
                target_bucket.upload_file(dest_dir, object_name)

        print(data, end = '')
