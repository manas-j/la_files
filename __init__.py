from main import get_knowledge_base, fetch_and_prcs
import pandas as pd
import uvicorn
import pyodbc
import pymongo
import faiss
import json
import numpy as np
import pandas as pd
from scipy import spatial
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import openai
import tiktoken
from pymongo import MongoClient
from bson import BSON
from bson.json_util import loads, dumps
import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
import requests


app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

server = 'sql-fullstack-sandbox-ci-001.database.windows.net'
database = 'lvlaccess_embeddings'
username = 'sqladmin'
password = 'Chandansql*'
driver= 'ODBC Driver 17 for SQL Server;'

cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

txt, embd, meta = get_knowledge_base()

openai.api_key = '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'

index = faiss.IndexFlatL2(len(embd[0]))
index.add(np.array(embd))



# Create the QueueClient object
# We'll use this object to create and interact with the queue
url = 'https://sam1cin001.queue.core.windows.net/queue-mail-m1-cin-001'
queue_name = 'queue-mail-m1-cin-001'
sas_tokn = '?sv=2022-11-02&ss=bq&srt=sco&sp=rwdlacupiytfx&se=2023-10-03T20:50:52Z&st=2023-09-04T12:50:52Z&spr=https,http&sig=8dQCWp1dWB%2B1FJrqbff9DZBIzVE8h2nBFLaRcxdNyPY%3D'
queue_client = QueueClient(url, queue_name=queue_name, credential=sas_tokn)


CONNECTION_STRING = 'mongodb+srv://FullStackAdmin:Hello%4012345@mongo-rfp-sandbox-eus-001.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000'
DB_NAME = "foundationalai"
COLLECTION_NAME = "documents"
# Create database if it doesn't
client = pymongo.MongoClient(CONNECTION_STRING)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
docs = db.documents


'''def get_opt_doc(inpt_id,collection):
    docs = []
    for doc in collection.find(inpt_id).sort(
            "email", pymongo.ASCENDING
    ):
        docs.append(docs)
    return docs'''

'''
def write_opt(inpt_id,collection):
    product = {
        "category": inpt_id['type'],
        "data": inpt_id['data'],
        "email": inpt_id['email'],
        "metadata1": inpt_id['metadata1'],
    }
    result = collection.update_one(
        {"name": product["email"]}, {"$set": product}, upsert=True
    )
    return result.upserted_id '''
def get_mongo_client():
    global client
    try:
        # Ping the MongoDB server to check the connection
        client.server_info()
        logging.info('MongoDB Connection Successfull')
        return client
    except pymongo.errors.ServerSelectionTimeoutError:
        logging.error("MongoDB connection failed.")
        raise Exception('Failed to connect to MongoDB.')

def warmup_function():
    # Replace 'YOUR_FUNCTION_URL' with the URL of your Queue Trigger function
    function_url = url
    for _ in range(3):  # Send 3 warm-up requests
        response = requests.post(function_url)
        logging.info(f'Warm-up request sent to {function_url}. Status code: {response.status_code}')


def main(msg: func.QueueMessage) -> None:
    try:
        logging.info('Python queue trigger function processed a queue item: %s', msg.get_body().decode('utf-8'))

        # try:
        # # Ping the MongoDB server to check the connection
        #     client.server_info()
        #     logging.info("MongoDB connection successful.")
        # except pymongo.errors.ServerSelectionTimeoutError:
        #     logging.error("MongoDB connection failed.")
        #     raise Exception('Failed to connect to MongoDB.')
        global client
        if not client or not client.is_mongos:
            client = get_mongo_client()

        bd = msg.get_body().decode('utf-8')
        # Receive a message
        received_message = queue_client.receive_messages()

        # Process the message

        # After processing, delete the message
        email_new = fetch_and_prcs(bd, txt, meta, embd, index) 
        if email_new==None:
            logging.info('Data Didnt get generated')
            raise Exception('Data Didnt get generated')
        else:
            logging.info('Data got generated')    
        '''
        result = collection.update_one(
            {'UserId': bd['userId'], 'DocumentID': bd['DocumentId']},
            {"$set": {"Summary": str(sum), 'Status': int(1), "Result": str(json.dumps(dct))}}
        )
        '''
        if email_new != None:
             logging.info('Data Got Updated on Mongo')

        else:
           logging.info('Data Didnt Got Updated on Mongo')
           raise Exception('Data not Updated on Mongo')  
           
        logging.info('Task got executed successfully')
        if received_message:
            queue_client.delete_message(message_id=received_message.message_id, pop_receipt=received_message.pop_receipt)
    except:
        logging.info('Function Failed to execute')
    


warmup_function()