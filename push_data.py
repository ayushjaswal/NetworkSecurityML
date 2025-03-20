import os
import sys
import json
import certifi
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pymongo
from NetworkSecurity.exception.exception import NetworkSecurityException
from NetworkSecurity.logging.logger import logging

load_dotenv(".env")

mongo_uri = os.getenv("MONGO_URI")

ca = certifi.where()

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_converter(self, filpath):
        try:
            data = pd.read_csv(filpath)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def insert_data_to_mongo(self, records, collection, database ):
        try:
            self.database = database
            self.records = records
            self.collection = collection

            self.mongo_client = pymongo.MongoClient(mongo_uri)
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]

            self.collection.insert_many(self.records)

            return len(self.records)

        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    FILE_PATH = "./Network_Data/phisingData.csv"
    DATABASE = "AYUSHAI"
    COLLECTION = "NetowrkSecurity"
    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json_converter(filpath=FILE_PATH)
    no_of_records = networkobj.insert_data_to_mongo(records=records, database=DATABASE, collection=COLLECTION)
    print(f"RECORDS: {records}\nLength: {no_of_records}")
