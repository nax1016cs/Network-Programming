import socket
import threading

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
		    if not data:
		    	break  
		    print('recive:',data.decode())
		    # self.socket.send(data.upper())
		self.socket.close()




if __name__ == "__main__":
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