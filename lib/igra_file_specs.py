#!/usr/bin/python3

data_specs = {
    "station_list": {
        #"source_url": "ftp://ftp.ncdc.noaa.gov/pub/data/igra/igra-stations.txt",
        "source_url": "file:///home/ninneman/laptop/devel/igra/stuff/igra-stations.txt",

        "table_sql": """
        create table if not exists station_list
        (country_code, station_number PRIMARY KEY, station_name, latitude, longitude, elevation, GUAN_code, LKS_code, composite_code, first_year, last_year)
        """,

        "ingest_sql": """
        replace into station_list values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,

        "field_spec": (
            (0, 2, str),
            (4, 9, int),
            (11, 46, str),
            (47, 53, float),
            (54, 61, float),
            (62, 66, int),
            (67, 68, str),
            (68, 69, str),
            (69, 70, str),
            (72, 76, int),
            (77, 81, int),
        ),
    },

    "country_list": {
        #"source_url": "ftp://ftp.ncdc.noaa.gov/pub/data/igra/igra-countries.txt",
        "source_url": "file:///home/ninneman/laptop/devel/igra/stuff/igra-countries.txt",

        "table_sql": """
        create table if not exists country_list
        (country_code PRIMARY KEY, country_name)
        """,

        "ingest_sql": """
        replace into country_list values (?, ?)
        """,

        "field_spec": (
            (0, 2, str),
            (5, 44, str),
        ),
    },

    "composite_stations": {
        #"source_url": "ftp://ftp.ncdc.noaa.gov/pub/data/igra/igra-composites.txt",
        "source_url": "file:///home/ninneman/laptop/devel/igra/stuff/igra-composites.txt",

        "table_sql": """
        create table if not exists composite_stations
        (c_number PRIMARY KEY, c_first, c_last, s1_number, s1_first, s1_last, s2_number, s2_first, s2_last)
        """,

        "ingest_sql": """
        replace into composite_stations values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,

        "field_spec": (
            (0, 5, int),
            (6, 16, str),
            (17, 27, str),
            (29, 34, int),
            (35, 45, str),
            (46, 56, str),
            (58, 63, int),
            (64, 74, str),
            (75, 86, str),
        ),
    },
}
