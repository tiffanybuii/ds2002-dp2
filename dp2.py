#!/usr/bin/env python3

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

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
    print(f)
    # do other things with f

# To import a single JSON file into MongoDB
# assuming you have defined a connection to your db and collection already:

    # Loading or Opening the json file
    try:
      file_data = json.load(f)
    except Exception as e:
      print(e, "error when loading", f)
     
# Inserting the loaded data in the collection
# if JSON contains data more than one entry
# insert_many is used else insert_one is used
    if isinstance(file_data, list):
      try:
        collection.insert_many(file_data)
      except Exception as e:
        print(e, "when importing into Mongo")
    else:
      try:
        collection.insert_one(file_data)
      except Exception as e:
        print(e)