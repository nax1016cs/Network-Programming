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

topic = ['tim']
is_running = True

class thread_sub(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.time_ = str(int(time.time()))[-6:]
        self.consumer = KafkaConsumer(group_id= self.time_, bootstrap_servers= ['localhost:9092'], api_version = (0, 9))
        

    def run(self):
        global topic, is_running
        print('thread running')
        while is_running == True :
            time.sleep(2)
            print("11111")
            # print(is_running)
            print(topic)
            self.consumer.subscribe(topics = topic)
            self.consumer.subscribe(topics = topic)
            for msg in self.consumer:
                print(msg)
                # if self.running == False:
                #     break
                # print(msg)
                # print('topic: ', self.topic)
                # print('keyword: ', self.keyword)

                # if self.sub_type == 'board':
                #     for word in sub_board[self.topic]:
                #         if self.keyword in word:
                #             print(msg.key.decode(), msg.value.decode())

                # elif self.sub_type == 'author':
                #     for word in sub_author[self.topic]:
                #         if self.keyword in word:
                #             print(msg.key.decode(), msg.value.decode())
            
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
    thread_sub().start() 
    while True:
        #prompt
        # time.sleep(0.5)
        find = False
        print('% ', end = '')
        input_str = input()
        input_split = input_str.split()
        if input_str.strip() == '':
            continue
        if input_str == 'exit':
            is_running = False

        client.send(input_str.encode())
        data = client.recv(4096).decode()

        if data.strip() == 'exit':
            client.close() 
            print('main close')
            print(is_running)
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

                topic.append(board_name)
                # if begin_thread:
                #     thread_sub().start()  
                #     begin_thread = False



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
                topic.append(author)
                # if begin_thread:
                #     thread_sub().start()  
                #     begin_thread = False
                    


            else:
                print("usage: subscribe --board <board-name> --keyword <keyword>")  
                continue  
            print(sub_author)
            print(sub_board)
            print('topic', topic)

        elif input_split[0] == 'list-sub':
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