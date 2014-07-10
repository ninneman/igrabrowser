#!/usr/bin/python3

import sys
import os

# load internal libraries first
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.pardir, "lib")))

import logging
import ingest
import igra_file_specs

# configure the logging
logger = logging.getLogger("IGRA browser")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)

# ingest the IGRA data
for source_name, data_spec in igra_file_specs.data_specs.items():
    ingest.ingest(data_spec)
