#!/usr/bin/python3

import sys
import os

# load internal libraries first
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.pardir, "lib")))

import sqlite3
import urllib.request
import logging
import db

def get_text_lines(url):
    r = urllib.request.urlopen(url)
    block = r.read().decode("ascii")
    lines = block.split("\n")
    return lines

def convert(data, dest_type):
    logger = logging.getLogger("IGRA browser")

    if dest_type == str:
        return str(data)
    elif dest_type == int:
        return int(data)
    elif dest_type == float:
        return float(data)
    else:
        logging.critical("Unrecognized type {0} given for input string {1}".format(str(dest_type), data))
        sys.exit(1)

def parse_fields(line, field_spec):
    row = []
    for field in field_spec:
        start_index = field[0]
        end_index = field[1]
        data_type = field[2]
        row.append(convert(line[start_index:end_index], data_type))

    return row

def ingest(spec):
    logger = logging.getLogger("IGRA browser")

    logger.info("retrieving {0}".format(spec["source_url"]))
    lines = get_text_lines(spec["source_url"])
    
    row_list = []
    
    for line in lines:
        # ignore blank lines
        if line == "":
            continue

        row = parse_fields(line, spec["field_spec"])
        row_list.append(row)

    logger.info("loading into database")
    conn = db.get_db_connection()
    logger.debug(spec["table_sql"])
    conn.execute(spec["table_sql"])
        
    for row in row_list:
        conn.execute(spec["ingest_sql"], row)

    conn.commit()
    conn.close()
