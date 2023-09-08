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

txt, embd, meta = get_knowledge_base()

openai.api_key = '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'

def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

index = faiss.IndexFlatL2(len(embd[0]))
index.add(np.array(embd))

def generate_indices(txt_lst,meta_data,embd_lst,ques,index):
    response = openai.Embedding.create(
      model = 'text-embedding-ada-002',
        input = [ques]
    )
    txt_embd = response['data'][0]['embedding']
    dist, ind = index.search(np.array(txt_embd).reshape(1,-1), 10)
    txts = []
    for i in ind[0]:
        txts.append(txt_lst[i])

    return txts,ind

def gen_ques(mbody):
    prompt = 'From the given email body, extract the questions that are asked by the sender to the receiver. Make sure that the questions generated follow the email wording strictly and are returned in the form of a list without an index.'
    prompt += 'Email body: """' + mbody + '"""'
    response = openai.Completion.create(
    model='text-davinci-003', 
    prompt=prompt, 
    max_tokens=256,
    temperature=0.3,
    stop=[' END'])
    mbody_ext =  response["choices"][0]["text"].strip()
    mbody_ext = mbody_ext.strip("[]")
    ls = [i.strip("''") for i in mbody_ext.split(", ")]
    return ls

def return_opt_prov(ques,predt,embd,idcs,meta):
    embd = np.array(embd)
    prompt = 'Context : """'
    for i in predt:
        prompt+=i+"\n"
    prompt += '"""'
    prompt += instruct + 'Question: """' + ques + '"""'

    response = openai.Completion.create(
        model='text-davinci-003', 
        prompt=prompt, 
        max_tokens=256,
        temperature=0.3,
        stop=[' END'])

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

CONNECTION_STRING = 'mongodb+srv://FullStackAdmin:Hello%4012345@mongo-rfp-sandbox-eus-001.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000'
DB_NAME = "foundationalai"
COLLECTION_NAME = "documents"
# Create database if it doesn't
client = pymongo.MongoClient(CONNECTION_STRING)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
docs = db.documents

def fetch_mongoID():
    queue_url = 'https://sam1cin001.queue.core.windows.net/queue-mail-m1-cin-001'
    queue_name = 'queue-mail-m1-cin-001'
    SaS_credential = '?sv=2022-11-02&ss=bq&srt=sco&sp=rwdlacupiytfx&se=2023-10-03T20:50:52Z&st=2023-09-04T12:50:52Z&spr=https,http&sig=8dQCWp1dWB%2B1FJrqbff9DZBIzVE8h2nBFLaRcxdNyPY%3D'
    connection_string = 'BlobEndpoint=https://sam1cin001.blob.core.windows.net/;QueueEndpoint=https://sam1cin001.queue.core.windows.net/;FileEndpoint=https://sam1cin001.file.core.windows.net/;TableEndpoint=https://sam1cin001.table.core.windows.net/;SharedAccessSignature=' + SaS_credential
    queue_client = QueueClient.from_connection_string(connection_string, queue_name, SaS_credential)
    if(len(message)>0):
        message = queue_client.peek_messages()[0]
        return message["id"]
    if(len(message)==0):
        return None
    
def prcs(MongoID, text_lst, meta_lst, embd_lst, index):
    if MongoID == None:
        return None
    email_bson = docs.find_one({"_id":MongoID})
    filter = {"_id": MongoID}
    #email_json = json.loads(email_bson)
    ans_ext_id = []
    for i in range(len(email_bson["Mail"]["Message"])):
        m_body = str(email_bson["Mail"]["Message"][i]["MailBody"])
        ls = gen_ques(m_body)
        ques_indcs = []
        indcs = []
        dic = {}
        for j in ls:
            instruct = 'You are a sales employee at LevelAccess. From the above given context answer the below question from a potential customer to the best of your knowledge. The answer should strictly be from the context and if you dont know or are unsure about it then just say "I Dont Know". Only refer to levelaccess when you answer the question and not Essential Accessibility. Assume that by "you" or "your" in the question, they mean LevelAccess. Also pay special attention to any corporation names that are mentioned and only include them in the answer if they are present in the contect. Follow the question wording strictly and donot use synonyms. Donot mention "Answer" in the beginning of the response.\n'
            text, ind = generate_indices(text_lst,meta_lst,embd_lst,j,index)
            predts, indpt, conf, score = return_opt_prov(j,text,embd_lst,ind,meta_lst)
            ref = ''
            for k in indpt:
                ref += k
            #print(ref)
            dic[j] = {"Answer": predts[0], "ConfidenceScore": conf, "References": ref}

        email_bson["Mail"]["Message"][i]["DictQnA"] = dic
        email_new = email_bson
    docs.update_one(filter,{"$set" : email_new})
    return email_new

@app.get("/")
async def root():
    mongo_id = fetch_mongoID()
    email_new = prcs(mongo_id, txt, meta, embd, index)
    return None