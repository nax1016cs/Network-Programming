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
                        Password TEXT NOT NULL );''')
        except sqlite3.OperationalError:
            pass
        conn.commit()
        conn.close()

    def insert(self, name, email, password, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute("INSERT INTO user ( Username , Email , Password) \
                   VALUES (?, ?, ? )" , (name, email, password ))
            socket.send("Register successfully.\n\r".encode())

        except sqlite3.IntegrityError:
            socket.send("Username is already used.\n\r".encode())
        conn.commit()
        conn.close()

    def select(self, name, password, socket):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT COUNT(*) FROM user WHERE Username = (?) AND Password = (?)", (name, password,))
        for row in cursor:
            return row[0]
        conn.commit()
        conn.close()

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

    # def insert(self, name, mod, socket):
    #     conn = sqlite3.connect('bbs.db')
    #     c = conn.cursor()
    #     try:
    #         c.execute("INSERT INTO board ( Board_name , Moderator) \
    #                VALUES (?, ?)" , (name, mod))
    #         socket.send("Create board successfully.\n\r".encode())

    #     except sqlite3.IntegrityError:
    #         socket.send("Board already exist.\n\r".encode())
    #     conn.commit()
    #     conn.close()

    def insert(self, name, mod):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute("INSERT INTO board ( Board_name , Moderator) \
                   VALUES (?, ?)" , (name, mod))
            

        except sqlite3.IntegrityError:
            # need to fix error message
            print('board_error')
        conn.commit()
        conn.close()

    def select(self, key):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM board  where Board_name like '%{}%' " .format(key))
        print('Index\tName\tModerator')
        i = 1
        for row in cursor:
            print('{index}\t{name}\t{mod}' .format( index = i, name = row[1] , mod = row[2]))
            i += 1
            # return row[0]
        conn.commit()
        conn.close()


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

    def list_post(self, bname ,key):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM post  where Board_name = (?) and Title like '%{}%' " .format(key),(bname,))
        print('ID\tTitle\tAuthor\tDate')
        for row in cursor:
            print('{id}\t{author}\t{title}\t{date}' .format( id = row[0], author = row[1] , title = row[3], date = row[2]))

            # return row[0]
        conn.commit()
        conn.close()

    def read_post(self, id):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM post  where PID = {} " .format(id))
        for row in cursor:
            print('Author\t:{}\nTitle\t:{}\nDate\t:{}' .format(row[1], row[3], row[2]))
            print('--')
            print(row[4])
            # return row[0]
        conn.commit()
        conn.close()
 
class comment_db():

    def __init__(self):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        try:
            c.execute('''CREATE TABLE comment
                      ( CID INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL UNIQUE,
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

    def list_comment(self, pid):
        conn = sqlite3.connect('bbs.db')
        c = conn.cursor()
        cursor = c.execute("SELECT * FROM comment  where Post_id = {} " .format(pid))
        print('--')
        for row in cursor:
            print('{name}: {content}' .format( name = row[1], content = row[2]))

            # return row[0]
        conn.commit()
        conn.close()

if __name__ == "__main__":
    bd = board_db()
    pt = post_db()
    ct = comment_db()

    pt.insert('ttt', 'author', 'title', 'date', '1')
    pt.list_post('b','t')
    pt.read_post(11)

    ct.insert('john', 'is so handsome', 11)
    ct.list_comment(11)



    # bd.insert('b1', 'jj')