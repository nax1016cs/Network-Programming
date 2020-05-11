# coding=utf-8
import sqlite3
import sys
import time
import os

path = '/home/ubuntu/Desktop/Network-Programming/hw3/tmp/'

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
            message = "Register successfully.\n\r"
            socket.send(message.encode())
            # need to fix 
            time.sleep(0.5)
            socket.send(B_name.encode())


        except sqlite3.IntegrityError:
            message = "Username is already used.\n\r" 
            socket.send(message.encode())
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
    
    def get_bucket(self, name):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        bucket_name = ""
        cursor = c.execute("SELECT Bucketname FROM user WHERE Username = (?)", (name,))
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
            message = "Create board successfully.\n\r" 
            socket.send(message.encode())

        except sqlite3.IntegrityError:
            message  = "Board already exist.\n\r" 
            socket.send(message.encode())
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

        message = 'Index\tName\t\tModerator\n\r'
        i = 1
        for row in cursor:
            message += '{:<8}{:<16}{}\n\r'.format(  i, row[1] ,  row[2])
            i += 1
        time.sleep(0.5)
        socket.send(message.encode())
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
                        Board_name TEXT NOT NULL,
                        Object_id  TEXT NOT NULL,
                        Userbucket TEXT NOT NULL
                        );''')
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def insert(self, Board_name,  Author, Date, Title, Content,userbucket ):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            object_name = "0516097-object" +  str(int(time.time())) + '.txt'
            new_content = '--\n\r' + Content + '\n\r--\n\r'
            with open(os.path.join(path,object_name), "w+") as file:
                file.write(new_content)
                file.close()
            c.execute("INSERT INTO post ( Board_name , Author, Date, Title, Object_id, Userbucket ) \
                   VALUES (?, ?, ?, ?, ?, ?)" , (Board_name, Author, Date ,Title, object_name, userbucket,))

        except sqlite3.IntegrityError:
            # need to fix error message
            print('post_error')
        conn.commit()
        conn.close()
        return object_name

    def list_post(self, bname ,key, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        if len(key) != 0:
            cursor = c.execute("PRAGMA case_sensitive_like = true")
            temp_key = '%' + key + '%'
            cursor = c.execute("SELECT * FROM post  where Board_name = (?) and Title like ? " ,(bname,temp_key,))
        else:
            cursor = c.execute("SELECT * FROM post  where Board_name = (?)  " ,(bname,))
        message = 'ID\tTitle\t\tAuthor\t\tDate\n\r'
        for row in cursor:
            # print(row)

            ###### id board title name date content
            new_date = row[2][row[2].find('-') + 1:].replace('-','/')
            message += '{:<8}{:<16}{:<16}{}\n\r' .format(  row[0],  row[3] ,row[1], new_date)

        socket.send(message.encode())
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

        message = ""
        object_name = ""
        author_bucket = ""
        for row in cursor:
            message += 'Author\t:{}\nTitle\t:{}\nDate\t:{}\n\r' .format(row[1], row[3], row[2])
            # print(row)
            object_name = row[5]
            # postid = int(row[0])
            author_bucket = row[6]
            # print(row)

        conn.commit()
        conn.close()
        # get the content
        return message, object_name, author_bucket

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

    def get_bucket_and_oid(self, id):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT Object_id, Userbucket FROM post  WHERE PID = ?" ,(id,))
        for row in cursor:
            # print(row)
            conn.commit()
            conn.close()
            return row[0], row[1]


    def delete_post(self, id):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        c.execute("DELETE from post WHERE PID = ?" ,(id,) )
        conn.commit()
        conn.close()

    def update_post_title(self, id, data):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        c.execute("UPDATE post SET Title = (?) WHERE PID = (?)" , ( data, id,) )
        conn.commit()
        conn.close()


class mail_db():

    def __init__(self):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE mail
                      ( MID INTEGER PRIMARY KEY AUTOINCREMENT,
                        Sender TEXT NOT NULL,          
                        Receiver TEXT NOT NULL,                        
                        Date TEXT NOT NULL,
                        Subject TEXT NOT NULL, 
                        Object_id  TEXT NOT NULL,
                        Userbucket TEXT NOT NULL
                        );''')
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    ## reciever, sender, date, subject, content, bucket
    def insert(self, Receiver, Sender, Date, Subject, Content, Userbucket ):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        object_name = "0516097-mail" +  str(int(time.time())) + '.txt'
        with open(os.path.join(path,object_name), "w+") as file:
            file.write(Content + '\n\r')
            file.close()
        c.execute("INSERT INTO mail ( Receiver , Sender, Date, Subject, Object_id, Userbucket ) \
                VALUES (?, ?, ?, ?, ?, ?)" , (Receiver, Sender, Date ,Subject, object_name, Userbucket,))

        conn.commit()
        conn.close()
        return object_name

    def list_mail(self, receiver, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM mail where Receiver = (?)  " ,(receiver,))
        message = 'ID\tSubject\tFrom\tDate\n\r'
        idx = 1
        for row in cursor:
            # print(row)
            new_date = row[3][row[3].find('-') + 1:].replace('-','/')
            message += '{:<8}{:<8}{:<8}{}\n\r' .format(  str(idx) ,  row[4] ,row[1], new_date)
            idx += 1

        socket.send(message.encode())
        conn.commit()
        conn.close()

    def get_mail_data(self, id_, receiver, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM mail where Receiver = (?)  " ,(receiver,))


        message = ""
        object_name = ""
        author_bucket = ""
        idx = 1
        for row in cursor:
            # print(row)
            if int(id_) == int(idx):
                message += 'Subject\t:{}\nFrom\t:{}\nDate\t:{}\n\r' .format(row[4], row[1], row[3]) + '--\r\n'
                # print(message)
                # print(row)
                object_name = row[5]
                # postid = int(row[0])
                author_bucket = row[6]
                break
            idx += 1
            # print(row)

        conn.commit()
        conn.close()
        # get the content
        return message, object_name, author_bucket

    def delete_mail(self, receiver, id_):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor =  c.execute("SELECT * from mail WHERE Receiver = ?" ,(receiver,) )
        idx = 1
        for row in cursor:
            # print(row)
            if int(id_) == int(idx):
                table_id = int(row[0])
                c.execute("DELETE from mail WHERE MID = ?" ,(table_id,) )
                object_name = row[5]
                author_bucket = row[6]
                break
            idx += 1
            # print(row)
        conn.commit()
        conn.close()
        return  object_name, author_bucket


if __name__ == "__main__":
    bd = board_db()
    pt = post_db()
    user = user_db()
    conn = sqlite3.connect('bbs.db')
    c = conn.cursor()
    # print(pt.get_bucket_and_oid(1))
 