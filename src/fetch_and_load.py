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

def get_db_connection():
    conn = sqlite3.connect("/tmp/igrabrowser/igrabrowser.db")
    return conn

class IGRAIngester:

    source_url = None
    data_name = None
    field_spec = None

    def get_text_lines(self, url):
        r = urllib.request.urlopen(url)
        block = r.read().decode("ascii")
        lines = block.split("\n")
        return lines

    def ingest(self):
        logger.info("retrieving {0}".format(self.source_url))
        lines = self.get_text_lines(self.source_url)

        row_list = []
    
        for line in lines:
            # ignore blank lines
            if line == "":
                continue

            # apply field_spec somehow
            line_list.append(row)

        logger.info("loading into database")
        conn = get_db_connection()
        c = conn.cursor
        c.execute(self.table_sql)
        
        for row in line_list:
            c.execute(self.ingest_sql, row)
        conn.commit()
        c.close()

def ingest_country_list():
    logger.info("getting country list")
    lines = get_text_lines("ftp://ftp.ncdc.noaa.gov/pub/data/igra/igra-countries.txt")

    country_list = []

    for line in lines:
        # ignore blank lines
        if line == "":
            continue

        row = (
            line[0:2], # country code
            line[5:44], # country name
        )
        country_list.append(row)

    logger.info("loading country list")
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("""
    create table if not exists country_list
    (country_code PRIMARY KEY, country_name)
    """)

    for row in country_list:
        c.execute("""
        replace into country_list values (?, ?)
        """, row)

    conn.commit()
    c.close()
        
def ingest_station_list():
    logger.info("getting station list")
    lines = get_text_lines("ftp://ftp.ncdc.noaa.gov/pub/data/igra/igra-stations.txt")
    #lines = get_text_lines("file:///tmp/igrabrowser/igra-stations.txt")

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


    logger.info("loading station list")
    conn = get_db_connection()
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

source_url = "ftp://ftp.ncdc.noaa.gov/pub/data/igra/igra-stations.txt"
data_name = "station_list"
field_spec = (
    ("country_code", False, 0, 2, str),
    ("station_number", True, 4, 9, int),
    ("station_name", False, 11, 46, str),
    ("latitude", False, 47, 53, float),
    ("longitude", False, 54, 61, float),
    ("elevation", False, 62, 66, int),
    ("GUAN_code", False, 67, 68, str),
    ("LKS_code", False, 68, 69, str),
    ("composite_code", False, 69, 70, str),
    ("first_year", False, 72, 76, int),
    ("last_year", False, 77, 81, int),
)
table_sql = """
create table if not exists station_list
(country_code, station_number PRIMARY KEY, station_name, latitude, longitude, elevation, GUAN_code, LKS_code, composite_code, first_year, last_year)
"""
ingest_sql = """
replace into station_list values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
