# coding=utf-8
import socket
import threading
from database import user_db
from database import board_db
from database import post_db
import sqlite3
import sys
import re
import datetime
import time

user = user_db()
board = board_db()
post = post_db()



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
            # self.socket.send("% ".encode())
            data_in = self.socket.recv(2048)
            # print(data_in)
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
                    messgae = "login <username> <password>\n\r"
                    self.socket.send(messgae.encode())

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
                        time.sleep(0.5)
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
                else:
                    board.insert(data[1], self.user, self.socket)

            elif data[0] == "list-board" :
                if len(data) == 2 :
                    if data[1][0] == '#' and data[1][1] == '#':
                        board.list_board(data[1][2:], self.socket)
                else:
                    board.list_board('', self.socket)

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

                    # print('title: ', title, 'content: ', content)
                    now = datetime.datetime.now()
                    date = str(now.year) + '-' + str(now.month) + '-' + str(now.day)
                    object_name = post.insert(data[1], self.user, date, title, content, bucket)
                    #  fix 
                    message = "Create post successfully.\n\r"
                    self.socket.send(message.encode())
                    # need to fix 
                    time.sleep(0.5)

                    self.socket.send(object_name.encode())


            # list-post
            elif  data[0] == "list-post" :
                if not board.check_board(data[1]) :
                    message = "Board does not exist.\n\r" 
                    self.socket.send(message.encode())
                else:
                    if len(data) == 3 :
                        if data[2][0] =='#' and data[2][1] =='#':
                            post.list_post(data[1], data[2][2:], self.socket)
                        # print(data[2].strip('#'))

                    elif len(data) == 2 :
                        post.list_post(data[1], "" , self.socket)
            elif data[0] == "read":
                if not post.check_post(data[1]):
                    message = "Post does not exist.\n\r" 
                    self.socket.send(message.encode())
                else:
                    self.socket.send('Read_post'.encode())
                    meta_data, objectid, author_bucket = post.read_post(data[1], self.socket)
                    # print(objectid)
                    datas = objectid + ' ' + author_bucket
                    self.socket.send(meta_data.encode())
                    time.sleep(0.5)
                    self.socket.send(datas.encode())
                    # self.socket.send(objectid.encode())




                    ####
                    # if comment.check_comment(data[1]):
                    #     comment.list_comment(data[1], self.socket)

            elif data[0] == "comment":
                if len(self.user) == 0 :
                    message = "Please login first.\n\r" 
                    self.socket.send(message.encode())

                elif not post.check_post(data[1]):
                    message = "Post does not exist.\n\r"   
                    self.socket.send(message.encode())

                else:
                    t_comment = re.search('\d (.*)', data_in.decode()).group(1).rstrip()
                    cmt = self.user + ": " + t_comment + '\n\r'
                    oid, bucket_name = post.get_bucket_and_oid(data[1])
                    datas = oid + " " + bucket_name
                    message = "Comment successfully.\n\r" 
                    self.socket.send(message.encode())
                    time.sleep(0.5)
                    self.socket.send(datas.encode())
                    time.sleep(0.5)
                    self.socket.send(cmt.encode())



            elif data[0] == "delete-post":
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
                    oid, bucket_name = post.get_bucket_and_oid(data[1])
                    post.delete_post(data[1])
                    datas = oid + " " + bucket_name
                    message = "Delete successfully.\n\r" 
                    self.socket.send(message.encode())
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

                    if new_title != None:
                        post.update_post_title(data[1], new_title.rstrip())
                        print(new_title)
                        self.socket.send(' '.encode())
                    else:
                        content = new_content.replace('<br>', '\n').rstrip()
                        oid, bucket_name = post.get_bucket_and_oid(data[1])
                        datas = oid + " " + bucket_name
                        self.socket.send(datas.encode())
                        print('data: ', datas)
                        time.sleep(0.5)
                        self.socket.send(new_content.encode())
                        print('content: ', new_content)



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
    server.bind(('localhost',port)) 

    server.listen(10) 
    while True:
        (conn,addr) = server.accept() 
        print("New connection")
        thread_server(conn, addr).start()
