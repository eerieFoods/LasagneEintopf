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
URL = f"{os.getenv('BASE_URL')}&apikey={API_KEY}"


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

col_name = "OilPricesDaily"
db_collection = None


if col_name in db.list_collection_names():
    logging.info("Collection %s, already exists", col_name)
    db_collection = db[col_name]
else:
    logging.info("Creating new collection: %s", col_name)
    db_collection = db.create_collection(col_name)


# store API Date in database
if __name__ == "__main__":
    logging.debug("Starting Program")
    logging.debug("URL: %s", URL)
    response = requests.get(URL).json()


    logging.info("Successfully loaded data")
    logging.debug("Data: %s", response)

    # delete old documents
    if (col_name == "OilPricesDaily"):
        logging.debug("Starting to delete old data")
        db_collection.drop()
        logging.debug("Deleted old data")


    logging.debug("Starting to insert data into database")
    db_collection.insert_one(response)
    logging.info("Inserted data into database")


