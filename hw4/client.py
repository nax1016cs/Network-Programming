# coding=utf-8
import socket
import sys
import boto3
import os
import re
import time
import threading
from kafka import KafkaConsumer
from kafka import TopicPartition

sub_board = {}
sub_author = {}

topic = []
is_running = True

class thread_sub(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.time_ = str(int(time.time()))[-6:]
        self.consumer = KafkaConsumer(group_id= self.time_, bootstrap_servers= ['localhost:9092'],
                                      api_version = (0,9), metadata_max_age_ms = 500,
                                      auto_offset_reset  = 'latest', enable_auto_commit = True,
                                       auto_commit_interval_ms =1000, session_timeout_ms = 30000, )
        

    def run(self):
        global topic, is_running
        print('thread running')
        self.consumer.subscribe(topics = topic)
        while is_running:
            msg = self.consumer.poll(timeout_ms=5)
            if len(msg) != 0:
                print (msg)
                for keys, values in msg.items():
                    inform_topic = keys.topic   # topic
                    type_ = values[0].key.decode()
                    value = values[0].value.decode()
                    print('topic: ', inform_topic)
                    print('key ', type_)
                    print('value ', value)

                title = value.split('!@#$%')[1] # title

                if type_ == 'author':
                    board = value.split('!@#$%')[0] # board
                    
                    for word in sub_author[inform_topic]:
                        if word in title:
                            print("Board: ", board, "Title: ", title, "Author: ", inform_topic)
                            print('% ', end = '')
                
                elif type_ == 'board':
                    name = value.split('!@#$%')[0] # author
                    
                    for word in sub_board[inform_topic]:
                        if word in title:
                            print("Board: ", inform_topic, "Title: ", title, "Author: ", name)
                            print('% ', end = '')
            
        print('thread terminated')
        return



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
    path = '/home/ubuntu/Desktop/nctu_nphw3_demoNetwork-Programming/hw4/tmp/'
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
    begin_thread = True
    while True:

        find = False
        print('% ', end = '')
        input_str = input()
        input_split = input_str.split()
        if input_str.strip() == '':
            continue
        # if input_str == 'exit':

        #     is_running = False

        client.send(input_str.encode())
        data = client.recv(4096).decode()

        if data.strip() == 'exit':
            client.close() 
            is_running = False
            print('main close')
            print(is_running)
            thread.join()
            
            # time.sleep(3)
            # thread_sub.exit()
            break



        elif input_split[0] == 'subscribe':
            if len(input_split) != 5:
                print("usage: subscribe --board <board-name> --keyword <keyword>")
                continue

            elif len(current_bucket) == 0:
                print("Please login first.")
                continue

            elif input_split[1] == '--board' and input_split[3] == '--keyword':
                board_name = re.search('--board (.*) --keyword (.*)', input_str).group(1)
                key_word = re.search('--board (.*) --keyword (.*)', input_str).group(2)
                if board_name in sub_board:
                    for record_ky in sub_board[board_name]:
                        if record_ky == key_word:
                            print('Already subscribed.')
                            find = True
                    if not find:
                        sub_board[board_name].append(key_word)
                else:
                    sub_board[board_name] = []
                    sub_board[board_name].append(key_word) 

                
                if begin_thread:
                    topic.append(board_name)
                    thread = thread_sub()
                    thread.start() 
                    begin_thread = False
                    print('begin_thread')
                else:
                    topic.append(board_name)
                    thread.consumer.subscribe(topics = topic)
                    print('NEWLY SUB:',thread.consumer.subscription() )



            elif input_split[1] == '--author' and input_split[3] == '--keyword':
                author = re.search('--author (.*) --keyword (.*)', input_str).group(1)
                key_word = re.search('--author (.*) --keyword (.*)', input_str).group(2)
                if author in sub_author:
                    for record_ky in sub_author[author]:
                        if record_ky == key_word:
                            print('Already subscribed.')
                            find = True
                    if not find:
                        sub_author[author].append(key_word)
                else:
                    sub_author[author] = []
                    sub_author[author].append(key_word) 
                if begin_thread:
                    topic.append(author)
                    thread = thread_sub()
                    thread.start()  
                    begin_thread = False
                    print('begin_thread')
                else:
                    topic.append(author)
                    thread.consumer.subscribe(topics = topic)
                    


            else:
                print("usage: subscribe --board <board-name> --keyword <keyword>")  
                continue  
            print(sub_author)
            print(sub_board)
            print('topic', topic)

        elif input_split[0] == 'list-sub':
            if len(current_bucket) == 0:
                print("Please login first.")

            if len(sub_author) > 0: 
                print('*' * 15, 'Author', '*' * 15 )
                for name in sub_author:
                    print(name, ': ', end = '')
                    for keyword in sub_author[name]:
                        print(keyword, ', ', end = '')
                    print()
                print('*' * 15, 'Author', '*' * 15 )

            if len(sub_board) > 0:
                print('*' * 15,'Board', '*' * 15 )
                for name in sub_board:
                    print(name, ': ', end = '')
                    for keyword in sub_board[name]:
                        print(keyword, ', ', end = '')
                    print()
                print('*' * 15,'Board', '*' * 15 )


        

        elif data.strip() == 'Register successfully.':
            # create the bucket
            client.send(' '.encode())
            bucket = client.recv(4096).decode()
            s3.create_bucket(Bucket = bucket)


        elif data[:8] == 'Welcome,':
            client.send(' '.encode())
            bucket = client.recv(4096).decode()
            current_bucket = bucket


        elif data[:5] == 'Bye, ':
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