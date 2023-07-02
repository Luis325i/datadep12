from google.cloud.storage import Client
#from pymongo import MongoClient
import os
import yaml
from io import StringIO
import sqlalchemy as db
from sqlalchemy import text

with open ('Y:/0.Dev/Datapath/Proyecto/config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

class Utilities():

    #conectar a GCP
    def cliente_gcp():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["cloud_storage"]["path"]
        client = Client()
        return client

#    def cliente_mongo(DB_name):
#        CONNECTION_STRING = config["mongodb"]["connection_string"]
#        client = MongoClient(CONNECTION_STRING)
#        return client[DB_name]

    def cliente_mysql(DB_name):
        engine = db.create_engine(f'mysql://{config["mysql"]["user"]}:{config["mysql"]["pass"]}@{config["mysql"]["host"]}:{config["mysql"]["port"]}/{DB_name}')
        conn = engine.connect()
        return conn