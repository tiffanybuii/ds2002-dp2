#!/usr/bin/env python3

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

def dp2():
  imported_records = 0
  orphaned_records = 0
  corrupted_records = 0


  # connect to a db/collection in Atlas
  MONGOPASS = os.getenv('MONGOPASS')
  uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"

  client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
  # specify a database
  db = client.tnb6zdz
  # specify a collection
  collection = db.dp2

  # import a bundle of fifty (50) separate JSON files into a new collection within a MongoDB database.
  # Each file contains one or more record.
  # The data for import can be found in the data/ directory of this repository.

  directory = "data"

  for filename in os.listdir(directory):
    with open(os.path.join(directory, filename)) as f:
      # print(f)
      # do other things with f

  # To import a single JSON file into MongoDB
  # assuming you have defined a connection to your db and collection already:

      # Loading or Opening the json file
      try:
        file_data = json.load(f)
      
      
  # Inserting the loaded data in the collection
  # if JSON contains data more than one entry
  # insert_many is used else insert_one is used
        # for file in file_data:
        if isinstance(file_data, list):
          try:
            collection.insert_many(file_data)
            imported_records += len(file_data)
          except errors.DuplicateKeyError as e:
            print(e, "when importing into Mongo, Duplicate Key Error")
        else:
          try:
            collection.insert_one(file_data)
            imported_records += 1
          except Exception as e:
            print(e)
      except json.JSONDecodeError as e:
        print(e, "corrupted record", f)
        corrupted_records += 1
        if isinstance(file_data, list):
          orphaned_records += len(file_data) - corrupted_records
      except Exception as e:
          print(e, "error when loading", f)
        
  print("Records imported:", imported_records)
  print("Records orphaned:", orphaned_records)
  print("Records corrupted:", corrupted_records)

if __name__ == '__main__':
  dp2()