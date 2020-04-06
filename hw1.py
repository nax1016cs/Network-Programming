import socket
import threading
from database import database
import sqlite3



class thread_server(threading.Thread):

    def __init__(self, socket, addr):
        threading.Thread.__init__(self)
        self.socket = socket
        self.addr = addr

    def run(self):
        self.socket.send("********************************\n\r")
        self.socket.send("** Welcome to the BBS server. **\n\r")
        self.socket.send("********************************\n\r")

        while True:
            data = self.socket.recv(1024)
            if data.decode() == 'exit':
                break  
            print('recive:',data.decode())
            # self.socket.send(data.upper())
        self.socket.close()


if __name__ == "__main__":
    
    # db = database() 
    # db.delete()
    # db.create()
    # db.insert('Teddy', '123' , '456')
    # db.insert('BOB', '7897' , 'dsfaf')


    # cursor = c.execute("SELECT Username, Email, Password   FROM user ")
    # # print(cursor)
    # for row in cursor:
    #     print("Username", row[0])
    #     print("Email", row[1])
    #     print("Password", row[2])


    server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = socket.gethostname()
    port = 9090
    server.bind((host,port)) 
    # print(host, port)
    server.listen(10) 
    while True:
        (conn,addr) = server.accept() 
        print(conn,addr)
        print("New connection")
        thread_server(conn, addr).start()