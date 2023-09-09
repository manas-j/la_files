#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import pymongo
import faiss
import json
import fitz
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from scipy import spatial
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import openai
import requests
import tiktoken
from pymongo import MongoClient
from bson import BSON
from bson.json_util import loads, dumps
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
import base64
app = FastAPI()


# In[2]:


origins = ["*"]


# In[3]:


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# In[4]:

server = 'sql-fullstack-sandbox-ci-001.database.windows.net'
database = 'lvlaccess_embeddings'
username = 'sqladmin'
password = 'Chandansql*'
driver= 'ODBC Driver 17 for SQL Server;'

cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


# In[5]:


def get_knowledge_base():
    sql_select = "SELECT * FROM EMBEDDINGS_STORAGE_levelaccess"
    cursor.execute(sql_select)

    # Fetch all rows from the result set
    rows1 = cursor.fetchall()
    embd2 = []
    # Iterate over the rows and print the contents
    for row in rows1:
        embd2.append(row)
    
    txt_lst = []
    embd_lst = []
    meta_lst = []
    for i in embd2:
        txt_lst.append(i[0])
        embd_lst.append(json.loads(i[1]))
        meta_lst.append(i[2])
    
    return txt_lst, embd_lst, meta_lst


# In[6]:


txt, embd, meta = get_knowledge_base()


# In[98]:


openai.api_key = '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'


# In[8]:


def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens


# In[9]:


index = faiss.IndexFlatL2(len(embd[0]))
index.add(np.array(embd))


# In[91]:


def generate_indices(txt_lst,meta_data,embd_lst,ques,index):
    try: 
        response = openai.Embedding.create(
          model = 'text-embedding-ada-002',
            input = [ques]
        )
    except openai.error.APIError as e:
      #Handle API error here, e.g. retry or log
      print(f"OpenAI API returned an API Error: {e}")
      pass
    except openai.error.APIConnectionError as e:
      #Handle connection error here
      print(f"Failed to connect to OpenAI API: {e}")
      pass
    except openai.error.RateLimitError as e:
      #Handle rate limit error (we recommend using exponential backoff)
      print(f"OpenAI API request exceeded rate limit: {e}")
      pass

    txt_embd = response['data'][0]['embedding']
    dist, ind = index.search(np.array(txt_embd).reshape(1,-1), 10)
    txts = []
    for i in ind[0]:
        txts.append(txt_lst[i])

    return txts,ind


# In[90]:


def gen_ques(mbody):
    prompt = 'From the given email body, extract the questions that are asked by the sender to the receiver. Make sure that the questions generated follow the email wording strictly and are returned in the form of a list without an index.'
    prompt += 'Email body: """' + mbody + '"""'
    
    try:
        response = openai.Completion.create(
        model='text-davinci-003', 
        prompt=prompt, 
        max_tokens=256,
        temperature=0.3,
        stop=[' END'])

    except openai.error.APIError as e:
      #Handle API error here, e.g. retry or log
      print(f"OpenAI API returned an API Error: {e}")
      pass
    except openai.error.APIConnectionError as e:
      #Handle connection error here
      print(f"Failed to connect to OpenAI API: {e}")
      pass
    except openai.error.RateLimitError as e:
      #Handle rate limit error (we recommend using exponential backoff)
      print(f"OpenAI API request exceeded rate limit: {e}")
      pass

    mbody_ext =  response["choices"][0]["text"].strip()
    mbody_ext = mbody_ext.strip("[]")
    ls = [i.strip("''") for i in mbody_ext.split(", ")]
    return ls


# In[105]:


def return_opt_prov(ques,predt,embd,idcs,meta,instruct):
    embd = np.array(embd)
    prompt = 'Context : """'
    for i in predt:
        prompt+=i+"\n"
    prompt += '"""'
    prompt += instruct + 'Question: """' + ques + '"""'
    
    try:
        response = openai.Completion.create(
            model='text-davinci-003', 
            prompt=prompt, 
            max_tokens=256,
            temperature=0.3,
            stop=[' END'])
    except openai.error.APIError as e:
      #Handle API error here, e.g. retry or log
      print(f"OpenAI API returned an API Error: {e}")
      pass
    except openai.error.APIConnectionError as e:
      #Handle connection error here
      print(f"Failed to connect to OpenAI API: {e}")
      pass
    except openai.error.RateLimitError as e:
      #Handle rate limit error (we recommend using exponential backoff)
      print(f"OpenAI API request exceeded rate limit: {e}")
      pass

    predt = response["choices"][0]["text"].strip()
    atb = predt.replace("\n",'').split('.')
    embdest = []
    for i in atb:
        if len(i) != 0:
            response = openai.Embedding.create(
                    model = 'text-embedding-ada-002',
                    input = [i]
            )
            embeddings = response['data'][0]['embedding']
            embdest.append(embeddings)

    # txt_embd = response.embeddings[0]
    # t1 = embd[ind[0][0]]
    indcs = idcs
    agg_score = 0
    inpos = dict()
    for i in range(10):
        inpos[i] = 0
    for i in embdest:
        mxv = -1
        mid = -1
        for k,v in enumerate(indcs[0]):
            t1 = embd[v]
            scr = 1 - spatial.distance.cosine(i,t1)
            if scr>mxv:
                mxv = scr
                mid = k
        inpos[mid]+=1
        agg_score += mxv
    indpt = []
    predts = []
    predts.append(predt)
    ppm = 0
    for k,v in inpos.items():
        if v!=0:
            ppm+=1
            pt = meta[indcs[0][k]]
            print(type(pt))
            #print(v)
            pt = pt.strip('{}')
            pt = '{'+ pt + ', "Percent:" ' + str((int(v)/10)*100) + '%}'
            indpt.append(pt)
    stn = ''
    if agg_score/2*ppm>0.6:
        stn = 'High'
    elif 0.5<agg_score/ppm and agg_score/ppm<0.7:
        stn = 'Medium'
    else:
        stn = 'Weak'
    score = agg_score/2*ppm
    
    return predts, indpt, stn, score


# In[81]:


CONNECTION_STRING = 'mongodb+srv://FullStackAdmin:Hello%4012345@mongo-rfp-sandbox-eus-001.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000'
DB_NAME = "foundationalai"
COLLECTION_NAME = "usersEmail"
# Create database if it doesn't
client = pymongo.MongoClient(CONNECTION_STRING)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


# In[197]:


def fetch_messageID():
    queue_url = 'https://sam1cin001.queue.core.windows.net/queue-mail-m1-cin-001'
    queue_name = 'queue-mail-m1-cin-001'
    SaS_credential = '?sv=2022-11-02&ss=bq&srt=sco&sp=rwdlacupiytfx&se=2023-10-03T20:50:52Z&st=2023-09-04T12:50:52Z&spr=https,http&sig=8dQCWp1dWB%2B1FJrqbff9DZBIzVE8h2nBFLaRcxdNyPY%3D'
    connection_string = 'BlobEndpoint=https://sam1cin001.blob.core.windows.net/;QueueEndpoint=https://sam1cin001.queue.core.windows.net/;FileEndpoint=https://sam1cin001.file.core.windows.net/;TableEndpoint=https://sam1cin001.table.core.windows.net/;SharedAccessSignature=' + SaS_credential
    queue_client = QueueClient.from_connection_string(connection_string, queue_name, SaS_credential)
    
    messages = queue_client.receive_messages(max_messages = 1)
    for msg in messages:
        msg_str = msg.content
    enc_str = base64.b64decode(msg_str)
    enc_str = enc_str.decode('ascii').strip('{}').split(',')[1].strip('"MessageId":')
    return enc_str


def prcs(msgID, text_lst, meta_lst, embd_lst, index):
    if msgID == None:
        return None
    email_bson = collection.find_one({"MessageId":msgID})
    if email_bson == None:
        return None
    filter = {"MessageId": msgID}
    #email_json = json.loads(email_bson)
    snip = email_bson["Snippet"]
    ques = gen_ques(snip)
    for key in ques:
        ques_indcs = []
        indcs = []
        dic = {}
        instruct = 'You are a sales employee at LevelAccess. From the above given context answer the below question from a potential customer to the best of your knowledge. The answer should strictly be from the context and if you dont know or are unsure about it then just say "I Dont Know". Only refer to levelaccess when you answer the question and not Essential Accessibility. Assume that by "you" or "your" in the question, they mean LevelAccess. Also pay special attention to any corporation names that are mentioned and only include them in the answer if they are present in the contect. Follow the question wording strictly and donot use synonyms. Donot mention "Answer" in the beginning of the response.\n'
        text, ind = generate_indices(text_lst,meta_lst,embd_lst,key,index)
        predts, indpt, conf, score = return_opt_prov(key,text,embd_lst,ind,meta_lst,instruct)
        ref = ''
        for k in indpt:
            ref += k
        #print(ref)
        out_string = predts[0] + "ConfidenceScore: " + conf + "References: "+ ref
        dic[key] = {"Answer": out_string}

    email_bson["emailsQuestions"] = dic
    email_new = email_bson
    collection.update_one(filter,{"$set" : email_new})
    return email_new
# In[112]:
@app.get("/")
def process_email(
    msgId: str,
    txt: list,
    meta: list,
    embd: list,
    index: int
):
    print("Executing process_email function")
    email_result = prcs(msgId, txt, meta, embd, index)
    if email_result:
        return {"message": "Email processed successfully"}
    else:
        return {"message": "Email processing failed"}
    
def fetch_mongoID():
    print("Executing fetch_mongoID function")
    msgId = fetch_messageID()
    if msgId:
        process_email(msgId, txt, meta, embd, index)
        return {"msgId": msgId}
    else:
        return {"message": "No message found in the queue"}