import sqlite3
import sys
# global conn,c



class database():
    def __init__(self):
        conn = sqlite3.connect('hw1.db')
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
        conn = sqlite3.connect('hw1.db')
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
        conn = sqlite3.connect('hw1.db')
        c = conn.cursor()
        cursor = c.execute("SELECT COUNT(*) FROM user WHERE Username = (?) AND Password = (?)", (name, password,))
        for row in cursor:
            return row[0]
        conn.commit()
        conn.close()

    def delete(self):
        conn = sqlite3.connect('hw1.db')
        c = conn.cursor()
        c.execute("DELETE from user")
        conn.commit()
        conn.close()
 