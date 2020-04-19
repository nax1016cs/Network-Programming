import socket
import threading
from database import user_db
from database import board_db
from database import post_db
from database import comment_db
import sqlite3
import sys
import re
import datetime

user = user_db()
board = board_db()
post = post_db()
comment = comment_db()


class thread_server(threading.Thread):

    def __init__(self, socket, addr):
        threading.Thread.__init__(self)
        self.socket = socket
        self.addr = addr
        self.user = ""

    def run(self):

        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        self.socket.send("********************************\n\r".encode())
        self.socket.send("** Welcome to the BBS server. **\n\r".encode())
        self.socket.send("********************************\n\r".encode())

        while True:
            self.socket.send("% ".encode())
            data_in = self.socket.recv(2048)
            
            if not data_in:
                continue
            elif data_in.decode() == 'exit\r\n':
                break

            # print(data_in.decode())
            data = data_in.decode().split()


            if(data[0] == "register"):
            	if( len(data) != 4):
            		self.socket.send("Usage: register <username> <email> <password>\n\r".encode())
            	else:
            		user.insert(data[1], data[2], data[3], self.socket)
            
            elif(data[0] == "login"):

                if( len(data) != 3):
                    self.socket.send("login <username> <password>\n\r".encode())
                elif(len(self.user) == 0):

                    count = user.select(data[1], data[2], self.socket)
                    if(count == 0):
                        self.socket.send("Login failed.\n\r".encode())

                    elif(count == 1):
                        welcome_str = "Welcome, " + data[1] + ".\n\r"
                        self.socket.send(welcome_str.encode())
                        self.user = data[1]
                else:
                    self.socket.send("Please logout first.\n\r".encode())

            elif( data[0] == "logout" and len(data) == 1):

                if( len(self.user) == 0):
                    self.socket.send("Please login first.\n\r".encode())
                else:
                    bye_str = "Bye, " + self.user + ".\n\r"
                    self.socket.send(bye_str.encode())
                    self.user = ""

            elif( data[0] == "whoami" and len(data) == 1):

                if( len(self.user) == 0):
                    self.socket.send("Please login first.\n\r".encode())
                else:
                    user_str = self.user + ".\n\r"
                    self.socket.send(user_str.encode())

            elif( data[0] == "create-board" ):
                if( len(self.user) == 0):
                    self.socket.send("Please login first.\n\r".encode())
                else:
                    board.insert(data[1], self.user, self.socket)

            elif( data[0] == "list-board" ):
                if(len(data) == 2):
                    board.list_board(data[1].strip('#'), self.socket)
                else:
                    board.list_board('', self.socket)

            elif( data[0] == "create-post" ):
                if( len(self.user) == 0):
                    self.socket.send("Please login first.\n\r".encode())

                elif( not board.check_board(data[1])):
                    self.socket.send("Board does not exist.\n\r".encode())
                
                else:
                    title = re.search('--title (.*) --content', data_in.decode()).group(1)
                    t_content = re.search('--content (.*)', data_in.decode()).group(1)
                    content = t_content.replace('<br>', '\n')

                    # print('title: ', title, 'content: ', content)
                    now = datetime.datetime.now()
                    date = str(now.year) + '-' + str(now.month) + '-' + str(now.day)
                    post.insert(data[1], self.user, date, title, content)

                    self.socket.send("Create post successfully.\n\r".encode())

            # list-post
            elif( data[0] == "list-post" ):
                if( not board.check_board(data[1])):
                    self.socket.send("Board does not exist.\n\r".encode())
                else:
                    if(len(data) == 3):
                        post.list_post(data[1], data[2].strip('#'), self.socket)
                        print(data[2].strip('#'))

                    elif(len(data) == 2):
                        post.list_post(data[1], "" , self.socket)
            else:
                pass







        self.socket.close()
        conn.commit()
        conn.close()
        return


if __name__ == "__main__":

    port = 9090
    if( len(sys.argv) == 2):
        port = int(sys.argv[1])

    server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = socket.gethostname()
    server.bind((host,port)) 
    server.listen(10) 
    while True:
        (conn,addr) = server.accept() 
        print("New connection")
        thread_server(conn, addr).start()
