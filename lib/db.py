#!/usr/bin/python3

import sqlite3

def get_db_connection():
    conn = sqlite3.connect("/home/ninneman/laptop/devel/igra/stuff/igrabrowser.db")
    return conn
