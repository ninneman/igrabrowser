#!/usr/bin/python3

from ftplib import FTP
import sqlite3

lines = []

def get_lines(block):
    global lines
    lines.append(block.split("\n"))

ftp = FTP("ftp.ncdc.noaa.gov")
ftp.login()
ftp.cwd("/pub/data/igra")
ftp.retrlines("RETR igra-stations.txt", get_lines)
ftp.quit()

for line in lines:
    data_string = line[0]
    country_code = data_string[0:2]
    print(country_code)

#conn = sqlite3.connect("igrabrowser.db")
#c = conn.cursor()

#c.execute("""

#""")
