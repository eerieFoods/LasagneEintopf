import sys
import time

import requests
import os
from dotenv import load_dotenv
import logging
import pymongo

# Load .env
load_dotenv()

# Logging
FORMAT = "%(levelname)s - %(asctime)s - %(filename)s - %(name)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

# Constants
API_KEY = os.getenv("API_KEY")
radius = 10
URL = f"{os.getenv('BASE_URL')}?lat={os.getenv('LATITUDE')}&lng={os.getenv('LONGITUDE')}&apikey={API_KEY}&rad={radius}&type=all"


# Database stuff
db_client = pymongo.MongoClient(os.getenv("MONGO_URL"))
db_name = os.getenv("MONGO_DB_NAME")
db = None
if db_name in db_client.list_database_names():
    logging.info("Database %s already exists", db_name)
    db = db_client[db_name]
else:
    logging.info("Creating new database: %s", db_name)
    db = db_client[db_name]

col_name = "tankerkoenig"
db_collection = None
if col_name in db.list_collection_names():
    logging.info("Collection %s, already exists", col_name)
    db_collection = db[col_name]
else:
    logging.info("Creating new collection: %s", col_name)
    db_collection = db.create_collection(col_name)


# This is where the magic happens
if __name__ == "__main__":
    logging.debug("Starting Program")
    logging.debug("URL: %s", URL)
    response = requests.get(URL).json()
    success = response['ok']
    if not success:
        logging.error("Error while loading data")
        logging.error(response['message'])
        sys.exit(1)

    logging.info("Successfully loaded data")
    logging.debug("Data: %s", response)

    # Getting the data
    stations = response['stations']
    logging.debug("Found %s stations", len(stations))

    # Calculate the average prices
    # avgs = [diesel, e10, e5]
    avgs = [0, 0, 0]
    counts = [0, 0, 0]
    for station in stations:
        logging.debug("Processing station with id: %s", station['id'])
        diesel = station['diesel']
        if diesel is not None:
            avgs[0] += diesel
            counts[0] += 1
        e10 = station['e10']
        if e10 is not None:
            avgs[1] += e10
            counts[1] += 1
        e5 = station['e5']
        if e5 is not None:
            avgs[2] += e5
            counts[2] += 1

    avgs[0] = round(avgs[0] / counts[0], 3)
    avgs[1] = round(avgs[1] / counts[1], 3)
    avgs[2] = round(avgs[2] / counts[2], 3)
    logging.debug("Average prices: %s", avgs)
    logging.debug("Counts: %s", counts)

    data = {
        "time": time.time(),
        "avg_diesel": avgs[0],
        "avg_e10": avgs[1],
        "avg_e5": avgs[2],
        "stations": stations
    }

    logging.debug("Starting to insert data into database")
    db_collection.insert_one(data)
    logging.info("Inserted data into database")




