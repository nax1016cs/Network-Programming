# coding=utf-8
import socket
import sys
import boto3
import os
import re
import time


def get_bucket_obj(client, postdata):
    author_bucket = postdata.split()[1]
    object_name = postdata.split()[0]
    target_bucket = s3.Bucket(author_bucket)
    target_object = target_bucket.Object(object_name)
    return target_object, object_name, target_bucket


def get_comment(obj_name):
    dest_dir = path + obj_name
    f = open(dest_dir, 'r')
    lines = f.readlines()
    content = "".join(lines)
    last_char_index = [ m.end(0) for m in re.finditer('--', content)]
    comment = content[last_char_index[-1]:]
    f.close()
    return comment





if  __name__ == "__main__":
    path = '/home/ubuntu/Desktop/nctu_nphw3_demo/tmp/'
    client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    port = 9090
    if len(sys.argv) == 2 :
        port = int(sys.argv[1])
    host = '127.0.0.1'
    client.connect((host,port))

    data = client.recv(4096) 
    print(data.decode(), end = '')
    current_bucket = ''

    s3 = boto3.resource('s3')
    while True:
        #prompt
        # time.sleep(0.5)
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
            client.send(' '.encode())
            bucket = client.recv(4096).decode()
            s3.create_bucket(Bucket = bucket)


        elif data[:8] == 'Welcome,':
            client.send(' '.encode())
            bucket = client.recv(4096).decode()
            current_bucket = bucket


        elif data[:5] == 'Bye,':
            current_bucket = ''


        
        elif data.strip() == 'Create post successfully.':
            # file name and its dir
            client.send(' '.encode())
            object_name = client.recv(4096).decode()
            dest_dir = './tmp/' + object_name

            target_bucket = s3.Bucket(current_bucket)
            target_bucket.upload_file(dest_dir, object_name)

        
        elif data.strip() == 'Read_post':
            client.send(' '.encode())
            meta_data = client.recv(4096).decode()
            print(meta_data, end = "")
            client.send(' '.encode())
            postdata = client.recv(4096).decode()
            target_object, object_name, target_bucket = get_bucket_obj(client,postdata)
            object_content = target_object.get()['Body'].read().decode()
            print(object_content, end = "")
        
        elif data.strip() == 'Comment successfully.':
            client.send(' '.encode())
            postdata = client.recv(4096).decode()
            target_object, object_name, target_bucket = get_bucket_obj(client, postdata)
            object_content = target_object.get()['Body'].read().decode()
            client.send(' '.encode())

            #recieve comment
            comment = client.recv(4096).decode().strip() + '\n'
            dest_dir = './tmp/' + object_name
            with open(os.path.join(path,object_name), "a+") as file:
                    file.write(comment)
                    # file.close()
            target_bucket.upload_file(dest_dir, object_name)

        elif data.strip() == 'Delete successfully.':
            client.send(' '.encode())
            postdata = client.recv(4096).decode()
            target_object, object_name, target_bucket = get_bucket_obj(client, postdata)
            target_object.delete()
            del_path = path + object_name
            os.remove(del_path)

        elif data.strip() == 'Update successfully.':
            client.send(' '.encode())
            message = client.recv(4096).decode()
            if message.strip() == '':
                pass
            else:
                target_object, object_name, target_bucket = get_bucket_obj(client, message)
                client.send(' '.encode())
                content = client.recv(4096).decode()
                ### update the content
                comment = get_comment(object_name).strip()
                if len(comment.strip()) != 0:
                    new_content = '--\n\r' + content + '\n\r--\n\r' + comment + '\n\r'
                else:
                    new_content = '--\n\r' + content + '\n\r--\n\r'

                dest_dir = './tmp/' + object_name
                
                with open(os.path.join(path,object_name), "w+") as file:
                    file.write(new_content)
                    # file.close()
                target_bucket.upload_file(dest_dir, object_name)

        elif data.strip() == 'Sent successfully.':
            client.send(' '.encode())
            maildata = client.recv(4096).decode()
            target_object, object_name, target_bucket = get_bucket_obj(client, maildata)
            dest_dir = './tmp/' + object_name
            target_bucket.upload_file(dest_dir, object_name)
        
        elif data.strip() == 'Read-mail':
            client.send(' '.encode())
            meta_data = client.recv(4096).decode()
            print(meta_data, end = "")
            client.send(' '.encode())
            maildata = client.recv(4096).decode()
            
            target_object, object_name, target_bucket = get_bucket_obj(client,maildata)
            object_content = target_object.get()['Body'].read().decode()
            print(object_content, end = "")

        elif data.strip() == 'Mail deleted.':
            client.send(' '.encode())
            postdata = client.recv(4096).decode()
            target_object, object_name, target_bucket = get_bucket_obj(client, postdata)
            target_object.delete()
            del_path = path + object_name
            os.remove(del_path)

        if data.strip() != "" and  data.strip() != 'Read-mail' and  data.strip() != 'Read_post':
            print(data, end = '')
