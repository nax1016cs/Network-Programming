import sqlite3
import sys
# global conn,c

conn = sqlite3.connect('hw1.db')
c = conn.cursor()

class database():
    def __init__(self):
        self.create()

    def create(self):
       try:
         c.execute('''CREATE TABLE user
              ( UID INTEGER PRIMARY KEY AUTOINCREMENT,
                Username TEXT NOT NULL UNIQUE,
                Email TEXT NOT NULL,
                Password TEXT NOT NULL );''')
       except sqlite3.OperationalError:
         pass

    def insert(self, name, email, password):
       try:
         c.execute("INSERT INTO user ( Username , Email , Password) \
             VALUES (?, ?, ? )" , (name, email, password ))
       except sqlite3.IntegrityError:
         pass

    def delete(self):
       c.execute("DELETE  from user")

    def close_db(self):
        
        conn.commit()
        conn.close()

if __name__ == "__main__":
    pass
