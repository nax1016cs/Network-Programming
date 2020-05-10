# coding=utf-8
import sqlite3
import sys


class user_db():
    def __init__(self):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE user
                      ( UID INTEGER PRIMARY KEY AUTOINCREMENT,
                        Username TEXT NOT NULL UNIQUE,
                        Email TEXT NOT NULL,
                        Password TEXT NOT NULL,
                        Bucketname NOT NULL );''')
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def insert(self, name, email, password, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            B_name = '0516097bucket' + name 
            c.execute("INSERT INTO user ( Username , Email , Password, Bucketname) \
                   VALUES (?, ?, ?, ? )" , (name, email, password, B_name ))
            socket.send("Register successfully.\n\r".encode())
            ### send the bucket name to client
            socket.send(B_name.encode())


        except sqlite3.IntegrityError:
            socket.send("Username is already used.\n\r".encode())
        conn.commit()
        conn.close()

    def login(self, name, password, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        bucket_name = ""
        cursor = c.execute("SELECT Bucketname FROM user WHERE Username = (?) AND Password = (?)", (name, password,))
        for row in cursor:
            bucket_name = row[0]
        conn.commit()
        conn.close()
        return bucket_name
        
    def delete(self):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        c.execute("DELETE from user")
        conn.commit()
        conn.close()


class board_db():

    def __init__(self):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE board
                      ( BID INTEGER PRIMARY KEY AUTOINCREMENT,
                        Board_name TEXT NOT NULL UNIQUE,
                        Moderator TEXT NOT NULL);''')
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def insert(self, name, mod, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute("INSERT INTO board ( Board_name , Moderator) \
                   VALUES (?, ?)" , (name, mod))
            socket.send("Create board successfully.\n\r".encode())

        except sqlite3.IntegrityError:
            socket.send("Board already exist.\n\r".encode())
        conn.commit()
        conn.close()

    def list_board(self, key, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        if(len(key) != 0):
            
            cursor = c.execute("PRAGMA case_sensitive_like = true")
            temp_key = '%' + key +'%'
            cursor = c.execute("SELECT * FROM board  where Board_name like ? " ,(temp_key, ))
        else:
            cursor = c.execute("SELECT * FROM board ")

        socket.send('Index\tName\t\tModerator\n\r'.encode())

        i = 1
        for row in cursor:
            socket.send('{:<8}{:<16}{}\n\r'.format(  i, row[1] ,  row[2]).encode())
            i += 1
            # return row[0]
        conn.commit()
        conn.close()

    def check_board(self, name):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        # print('name:' , name , 'len:', len(name))
        try:
            cursor = c.execute("PRAGMA case_sensitive_like = true")
            cursor = c.execute("SELECT COUNT(*) FROM board WHERE Board_name = ? " ,(name,))
            for row in cursor:
                conn.commit()
                conn.close()
                # print(row)
                return row[0]
        except:
            conn.commit()
            conn.close()
            return 0
        

class post_db():

    def __init__(self):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE post
                      ( PID INTEGER PRIMARY KEY AUTOINCREMENT,
                        Author TEXT NOT NULL,                        
                        Date TEXT NOT NULL,
                        Title TEXT NOT NULL, 
                        Content TEXT NOT NULL,
                        Board_name TEXT NOT NULL );''')
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def insert(self, Board_name,  Author, Date, Title,  Content):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute("INSERT INTO post ( Board_name , Author, Date, Title, Content) \
                   VALUES (?, ?, ?, ?, ?)" , (Board_name, Author, Date ,Title, Content))
            # print('ok')

        except sqlite3.IntegrityError:
            # need to fix error message
            print('post_error')
        conn.commit()
        conn.close()

    def list_post(self, bname ,key, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        if len(key) != 0:
            cursor = c.execute("PRAGMA case_sensitive_like = true")
            temp_key = '%' + key + '%'
            cursor = c.execute("SELECT * FROM post  where Board_name = (?) and Title like ? " ,(bname,temp_key,))
        else:
            cursor = c.execute("SELECT * FROM post  where Board_name = (?)  " ,(bname,))
        socket.send('ID\tTitle\t\tAuthor\t\tDate\n\r'.encode())
        for row in cursor:
            # print(row)

            ###### id board title name date content
            new_date = row[2][row[2].find('-') + 1:].replace('-','/')
            socket.send('{:<8}{:<16}{:<16}{}\n\r' .format(  row[0],  row[3] ,row[1], new_date).encode())

            # return row[0]
        conn.commit()
        conn.close()

    def check_post(self, id):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        # print('name:' , name , 'len:', len(name))
        try:
            cursor = c.execute("SELECT COUNT(*) FROM post WHERE PID = ? " ,(id,))
            for row in cursor:
                conn.commit()
                conn.close()
                # print(row)
                return row[0]
        except:
            conn.commit()
            conn.close()
            return 0

    def read_post(self, id, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM post  where PID = ? " ,(id,))
        for row in cursor:
            # print(row)
            # socket.send('ID\tTitle\tAuthor\tDate\n\r'.encode())
            socket.send('Author\t:{}\nTitle\t:{}\nDate\t:{}\n\r' .format(row[1], row[3], row[2]).encode())
            socket.send('--\n\r'.encode())
            socket.send(row[4].encode())
            socket.send('\n\r--\n\r'.encode())

            # return row[0]
        conn.commit()
        conn.close()

    def get_user(self, id):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT Author FROM post  WHERE PID = ?" ,(id,))
        for row in cursor:
            # print(row)
            conn.commit()
            conn.close()
            # print(row)
            return row[0]


    def delete_post(self, id):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        c.execute("DELETE from post WHERE PID = ?" ,(id,) )
        conn.commit()
        conn.close()

    def update_post(self, id, target, data):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        c.execute("UPDATE post SET {} = (?) WHERE PID = (?)" .format(target), ( data, id,) )
        conn.commit()
        conn.close()
 
class comment_db():

    def __init__(self):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE comment
                      ( CID INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL,
                        Content TEXT NOT NULL,
                        Post_id INTEGER NOT NULL);''')
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def insert(self, name, content, pid):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute("INSERT INTO comment ( Name , Content, Post_id) \
                   VALUES (?, ?, ?)" , (name, content, pid))
            # print('ok')

        except sqlite3.IntegrityError:
            # need to fix error message
            print('comment_error')
            pass
        conn.commit()
        conn.close()

    def list_comment(self, pid, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM comment  where Post_id = ? " ,(pid,) )
        for row in cursor:
            # print(row)
            socket.send('{name}: {content}\n\r' .format( name = row[1], content = row[2]).encode())
            # return row[0]
        conn.commit()
        conn.close()

    def check_comment(self, id):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        # print('name:' , name , 'len:', len(name))
        try:
            cursor = c.execute("SELECT COUNT(*) FROM comment WHERE Post_id = ? " ,(id,) )
            for row in cursor:
                conn.commit()
                conn.close()
                # print(row)
                return row[0]
        except:
            conn.commit()
            conn.close()
            return 0

if __name__ == "__main__":
    bd = board_db()
    pt = post_db()
    ct = comment_db()
    user = user_db()
    conn = sqlite3.connect('bbs.db')
    c = conn.cursor()
    # bucket_name = ""
    # print('fadsf')
    bucket = ''
    c.execute("INSERT INTO user ( Username , Email , Password, Bucketname) \
                   VALUES (?, ?, ?, ? )" , ('bobbb', 'email', '123', 'this is testing' ))
    cursor = c.execute("SELECT Bucketname FROM user WHERE Username = (?) AND Password = (?)", ('bobbb', '123',))
    for row in cursor:
        # print(row)
        bucket = row[0]
    print(bucket)
 