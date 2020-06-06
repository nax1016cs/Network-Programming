# coding=utf-8
import socket
import threading
from database import user_db
from database import board_db
from database import post_db
from database import mail_db
import sqlite3
import sys
import re
import datetime
import time
from kafka import KafkaProducer

user = user_db()
board = board_db()
post = post_db()
mail = mail_db()



class thread_server(threading.Thread):

    def __init__(self, socket, addr):
        threading.Thread.__init__(self)
        self.socket = socket
        self.addr = addr
        self.user = ""
        self.bucket = ""

    def run(self):

        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        self.socket.send("********************************\n\r** Welcome to the BBS server. **\n\r********************************\n\r".encode())

        while True:
            data_in = self.socket.recv(2048)
            if not data_in:
                continue

            elif data_in.decode() == 'exit':
                self.socket.send('exit'.encode())
                break


            data = data_in.decode().split()

        
            if data[0] == "register":
            	if len(data) != 4:
                    message = "Usage: register <username> <email> <password>\n\r" 
                    self.socket.send(message.encode())
            	else:
            		user.insert(data[1], data[2], data[3], self.socket)
            
            elif data[0] == "login":

                if len(data) != 3:
                    message = "login <username> <password>\n\r"
                    self.socket.send(message.encode())

                elif len(self.user) == 0:
                    bucket = user.login(data[1], data[2], self.socket)
                    if len(bucket) == 0:
                        message = "Login failed.\n\r" 
                        self.socket.send(message.encode())
                    else :
                        # need to fix 
                        message = "Welcome, " + data[1] + ".\n\r"
                        self.user = data[1]
                        self.bucket = bucket
                        self.socket.send(message.encode())
                        t = self.socket.recv(4096).decode()
                        # time.sleep(0.2)
                        self.socket.send(bucket.encode())


                else:
                    message = "Please logout first.\n\r" 
                    self.socket.send(message.encode())

            elif data[0] == "logout" and len(data) == 1 :

                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())
                else:
                    message = "Bye, " + self.user + ".\n\r" 
                    self.socket.send(message.encode())
                    self.user = ""
                    self.bucket = ""

            elif data[0] == "whoami" and len(data) == 1 :

                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())
                else:
                    message = self.user + ".\n\r" 
                    self.socket.send(message.encode())

            elif data[0] == "create-board" :
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif len(data) != 2:
                    message = "create-board <name>\n\r" 
                    self.socket.send(message.encode())
                
                else:
                    board.insert(data[1], self.user, self.socket)

            elif data[0] == "list-board" :
                if len(data) == 2 :
                    if data[1][0] == '#' and data[1][1] == '#':
                        board.list_board(data[1][2:], self.socket)
                    else:
                        message = "list-board ##<key>\n\r" 
                        self.socket.send(message.encode())

                elif len(data) ==1:
                    board.list_board('', self.socket)
                
                else:
                    message = "list-board ##<key>\n\r" 
                    self.socket.send(message.encode())

            elif data[0] == "create-post" :
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif not board.check_board(data[1]) :
                    message = "Board does not exist.\n\r" 
                    self.socket.send(message.encode())
                
                else:
                    title = re.search('--title (.*) --content', data_in.decode()).group(1)
                    t_content = re.search('--content (.*)', data_in.decode()).group(1)
                    content = t_content.replace('<br>', '\n')

                    now = datetime.datetime.now()
                    date = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day)
                    object_name = post.insert(data[1], self.user, date, title, content, bucket)
                    message = "Create post successfully.\n\r"
                    self.socket.send(message.encode())
                    # need to fix 
                    # time.sleep(0.2)
                    t = self.socket.recv(4096).decode()
                    
                    self.socket.send(object_name.encode())
                    try:
                        future = producer.send(data[1], key= ('board').encode(), value= (self.user  + '!@#$%' + title).encode(), partition= 0)
                        print('sending board: ', data[1])
                        print(future)
                        # result = future.get(timeout= 10)
                        # print(result)
                        future = producer.send(self.user, key= ('author').encode(), value= (data[1] + '!@#$%' + title).encode(), partition= 0)
                        # result = future.get(timeout= 10)
                        print('sending author: ', self.user)
                        print(future)
                        producer.flush()

                    except:
                        pass


            # list-post
            elif  data[0] == "list-post" :
                if not board.check_board(data[1]) :
                    message = "Board does not exist.\n\r" 
                    self.socket.send(message.encode())
                else:
                    if len(data) == 3 :
                        if data[2][0] =='#' and data[2][1] =='#':
                            post.list_post(data[1], data[2][2:], self.socket)

                    elif len(data) == 2 :
                        post.list_post(data[1], "" , self.socket)
            elif data[0] == "read":
                if not post.check_post(data[1]):
                    message = "Post does not exist.\n\r" 
                    self.socket.send(message.encode())

                elif len(data) != 2:
                    message = "read <post-id>\n\r" 
                    self.socket.send(message.encode())

                else:
                    self.socket.send('Read_post'.encode())
                    meta_data, objectid, author_bucket = post.read_post(data[1], self.socket)
                    datas = objectid + ' ' + author_bucket
                    t = self.socket.recv(4096).decode()
                    self.socket.send(meta_data.encode())
                    t = self.socket.recv(4096).decode()
                    # time.sleep(0.2)
                    self.socket.send(datas.encode())
 

            elif data[0] == "comment":
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif not post.check_post(data[1]):
                    message = "Post does not exist.\n\r"   
                    self.socket.send(message.encode())

                else:
                    t_comment = re.search('\d (.*)', data_in.decode()).group(1).rstrip()
                    cmt = self.user + ": " + t_comment 
                    oid, bucket_name = post.get_bucket_and_oid(data[1])
                    datas = oid + " " + bucket_name
                    message = "Comment successfully.\n\r" 
                    self.socket.send(message.encode())
                    # time.sleep(0.2)
                    t = self.socket.recv(4096).decode()
                    self.socket.send(datas.encode())
                    t = self.socket.recv(4096).decode()
                    # time.sleep(0.2)
                    self.socket.send(cmt.encode())



            elif data[0] == "delete-post":
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())
                    
                elif len(data) != 2:
                    message = "delete-post <post-id>\n\r" 
                    self.socket.send(message.encode())

                elif not post.check_post(data[1]):
                    message = "Post does not exist.\n\r" 
                    self.socket.send(message.encode())

                elif self.user != post.get_user(data[1]):
                    message = "Not the post owner.\n\r" 
                    self.socket.send(message.encode())

                else:
                    oid, bucket_name = post.get_bucket_and_oid(data[1])
                    post.delete_post(data[1])
                    datas = oid + " " + bucket_name
                    message = "Delete successfully.\n\r" 
                    self.socket.send(message.encode())
                    t = self.socket.recv(4096).decode()
                    self.socket.send(datas.encode())


            elif data[0] == "update-post":
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif not post.check_post(data[1]):
                    message = "Post does not exist.\n\r" 
                    self.socket.send(message.encode())

                elif self.user != post.get_user(data[1]):
                    message = "Not the post owner.\n\r" 
                    self.socket.send(message.encode())

                else:
                    new_title = re.search('--title (.*)|--content (.*)', data_in.decode()).group(1)
                    new_content = re.search('--title (.*)|--content (.*)', data_in.decode()).group(2)
                    message = "Update successfully.\n\r" 
                    self.socket.send(message.encode())
                    t = self.socket.recv(4096).decode()
                    if new_title != None:
                        post.update_post_title(data[1], new_title.rstrip())
                        self.socket.send(' '.encode())
                    else:
                        content = new_content.replace('<br>', '\n').rstrip()
                        oid, bucket_name = post.get_bucket_and_oid(data[1])
                        datas = oid + " " + bucket_name
                        self.socket.send(datas.encode())
                        # time.sleep(0.2)
                        t = self.socket.recv(4096).decode()
                        self.socket.send(content.encode())

            elif data[0] == 'mail-to':
                receiver_bucket = user.get_bucket(data[1])
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif  len(receiver_bucket) == 0 :
                    message = data[1] + " does not exist.\n\r" 
                    self.socket.send(message.encode())
                
                else:
                    subject = re.search('--subject (.*) --content', data_in.decode()).group(1)
                    t_content = re.search('--content (.*)', data_in.decode()).group(1)
                    content = t_content.replace('<br>', '\n')

                    now = datetime.datetime.now()
                    date = str(now.year) + '-' + str(now.month).zfill(2) + '-' + str(now.day)
                    ## receiver, sender, date, subject, content, receiver_bucket
                    object_name = mail.insert(data[1], self.user, date, subject, content, receiver_bucket)
                    #  fix 
                    message = "Sent successfully.\n\r"
                    self.socket.send(message.encode())
                    # need to fix 
                    # time.sleep(0.2)
                    t = self.socket.recv(4096).decode()
                    datas = object_name + ' ' + receiver_bucket
                    self.socket.send(datas.encode())
            elif data[0] == 'list-mail':
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                else:
                    mail.list_mail(self.user, self.socket)
                    
            elif data[0] == 'retr-mail':
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif len(data) != 2:
                    message = "retr-mail <mail#>\n\r" 
                    self.socket.send(message.encode())

                else:
                    metadata, object_name, receiver_bucket = mail.get_mail_data(data[1], self.user ,self.socket)
                    if len(metadata) == 0:
                        message = 'No such mail.\n\r'
                        self.socket.send(message.encode())
                    else:
                        message = "Read-mail"
                        self.socket.send(message.encode())
                        # time.sleep(0.2)
                        t = self.socket.recv(4096).decode()
                        self.socket.send(metadata.encode())
                        # time.sleep(0.2)
                        t = self.socket.recv(4096).decode()
                        datas = object_name + ' ' + receiver_bucket
                        self.socket.send(datas.encode())

            elif data[0] == 'delete-mail':
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif len(data) != 2:
                    message = "delete-mail <mail#>\n\r" 
                    self.socket.send(message.encode())
                else:
                    metadata, object_name, receiver_bucket = mail.get_mail_data(data[1], self.user, self.socket)
                    if len(metadata) == 0 :
                        message = 'No such mail.\n\r'
                        self.socket.send(message.encode())
                    else:
                        object_name, receiver_bucket =  mail.delete_mail(self.user, data[1])
                        message = "Mail deleted.\n\r"
                        self.socket.send(message.encode())
                        # time.sleep(0.2)
                        t = self.socket.recv(4096).decode()
                        datas = object_name + ' ' + receiver_bucket
                        self.socket.send(datas.encode())
            else:
                self.socket.send(' '.encode())


        self.socket.close()
        conn.commit()
        conn.close()
        return


if __name__ == "__main__":

    port = 9090
    if len(sys.argv) == 2 :
        port = int(sys.argv[1])

    server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    # host = socket.gethostname()
    # print(host)
    # server.bind((host,port))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version = (0,9), metadata_max_age_ms  = 500)
    server.bind(('127.0.0.1',port)) 

    server.listen(10) 
    while True:
        (conn,addr) = server.accept() 
        print("New connection")
        thread_server(conn, addr).start()
