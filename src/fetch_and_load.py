#!/usr/bin/python3

import sqlite3
import urllib.request
import sys
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)

logger.info("getting station list")
#r = urllib.request.urlopen("ftp://ftp.ncdc.noaa.gov/pub/data/igra/igra-stations.txt")
r = urllib.request.urlopen("file:///tmp/igrabrowser/igra-stations.txt")
block = r.read().decode("ascii")
lines = block.split("\n")

station_list = []

for line in lines:
    # ignore blank lines
    if line == "":
        continue

    row = (
        line[0:2], # country code
        int(line[4:9]), # station number
        line[11:46], # station name
        float(line[47:53]), # latitude
        float(line[54:61]), # longitude
        int(line[62:66]), # elevation
        line[67:68], # GUAN code
        line[68:69], # LKS network code
        line[69:70], # composite station code
        int(line[72:76]), # first year of record
        int(line[77:81]), # last year of record
        )
    station_list.append(row)


logger.info("loading database")
conn = sqlite3.connect("/tmp/igrabrowser/igrabrowser.db")
c = conn.cursor()

c.execute("""
create table if not exists station_list
  (country_code, station_number PRIMARY KEY, station_name, latitude, longitude, elevation, GUAN_code, LKS_code, composite_code, first_year, last_year)
""")

for row in station_list:
    c.execute("""
    replace into station_list values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)

conn.commit()
c.close()
