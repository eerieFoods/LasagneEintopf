import os
import sys
import time
import random
import logging
import pymongo
import requests
from dotenv import load_dotenv
from constants import *

# Load .env
load_dotenv()

# Logging
logger = logging.getLogger(__name__)

FORMAT = "%(levelname)s - %(asctime)s - %(filename)s - %(name)s - %(message)s"
LOG_LEVEL = "DEBUG" if os.getenv("LOG_LEVEL") is None else os.getenv("LOG_LEVEL")

logger.setLevel(LOG_LEVEL)
logFormatter = logging.Formatter(FORMAT)
#logging.basicConfig(level=LOG_LEVEL, format=FORMAT)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


# Constants
API_KEY = os.getenv("API_KEY")
radius = 10
URL = f"{BASE_URL}?lat={LATITUDE}&lng={LONGITUDE}&apikey={API_KEY}&rad={radius}&type=all"

# Database stuff
db_client = pymongo.MongoClient(os.getenv("MONGO_URL"))
db_name = MONGO_DB_NAME
db = None
if db_name in db_client.list_database_names():
    logger.info("Database %s already exists", db_name)
    db = db_client[db_name]
else:
    logger.info("Creating new database: %s", db_name)
    db = db_client[db_name]

col_name = "tankerkoenig"
db_collection = None
if col_name in db.list_collection_names():
    logger.info("Collection %s, already exists", col_name)
    db_collection = db[col_name]
else:
    logger.info("Creating new collection: %s", col_name)
    db_collection = db.create_collection(col_name)


# Send a notification to Jan-Luca's phone
def notify_jay_on_home_assistant():
    logger.info("Sending Error-Notification to Jan-Luca")
    ha_api_key = os.getenv("HOME_ASSISTANT_API_KEY")
    url = "https://lupussmart.de/api/states/input_boolean.tankerkoenig_error"
    http_response = requests.post(url,
                                  data='{"state": "on"}',
                                  headers={"Authorization": f"Bearer {ha_api_key}", "content-type": "application/json"})
    logger.debug("Response from Home-Assistant: %s", http_response.json())


# This is where the magic happens
if __name__ == "__main__":
    try:
        while True:
            logger.debug("Starting Program")
            logger.debug("URL: %s", URL)
            response = requests.get(URL).json()
            success = response['ok']
            if not success:
                logger.error("Error while loading data")
                logger.error(response['message'])
                sys.exit(1)

            logger.info("Successfully loaded data")
            logger.debug("Data: %s", response)

            # Getting the data
            stations = response['stations']
            logger.debug("Found %s stations", len(stations))

            # Calculate the average prices
            # avgs = [diesel, e10, e5]
            avgs = [0, 0, 0]
            counts = [0, 0, 0]
            for station in stations:
                logger.debug("Processing station with id: %s", station['id'])

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
            logger.debug("Average prices: %s", avgs)
            logger.debug("Counts: %s", counts)

            data = {
                "time": time.time(),
                "avg_diesel": avgs[0],
                "avg_e10": avgs[1],
                "avg_e5": avgs[2],
                "stations": stations
            }

            logger.debug("Starting to insert data into database")
            db_collection.insert_one(data)
            logger.info("Inserted data into database")

            # Sleep for 30 min (1800 sec) + random offset because of the API rate limit
            # Sleeps for 25-35 min
            time_to_sleep = 1800 + random.randrange(-300, 300)
            logger.info("Sleeping for %s seconds", time_to_sleep)
            time.sleep(time_to_sleep)

    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            logger.info("Program was interrupted by user")
            sys.exit(0)

        logger.error("Error while running program")
        logger.error(e)
        notify_jay_on_home_assistant()
