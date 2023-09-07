#!/usr/bin/env python
# coding: utf-8 In[17]:
import pyodbc 
import pymongo 
import faiss 
import json 
import fitz 
import numpy as np 
import pandas as pd 
import os 
from bs4 import BeautifulSoup 
from scipy import spatial 
import time 
from fastapi import FastAPI 
from fastapi.middleware.cors import CORSMiddleware 
import openai 
import requests 
import tiktoken 
from pymongo import MongoClient 
from bson import BSON 
app = FastAPI()
# In[2]:
origins = ["*"]
# In[3]:
app.add_middleware( CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"], )
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
    for row in rows1: embd2.append(row)
    
    txt_lst = [] 
    embd_lst = [] 
    meta_lst = [] 
    for i in embd2: 
        txt_lst.append(i[0]) 
        embd_lst.append(json.loads(i[1])) 
        meta_lst.append(i[2])
    
    return txt_lst, embd_lst, meta_lst
# In[18]:
txt, embd, meta = get_knowledge_base()
# In[6]:
openai.api_key = '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'
# In[8]:
def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens
# In[9]:
instruct = 'You are a sales employee at LevelAccess. From the above given context answer the below question from a potential customer to the best of your knowledge. The answer should strictly be from the context and if you dont know or are unsure about it then just say "I Dont Know". Only refer to levelaccess when you answer the question and not Essential Accessibility. Assume that by "you" or "your" in the question, they mean LevelAccess. Also pay special attention to any corporation names that are mentioned and only include them in the answer if they are present in the contect. Follow the question wording strictly and donot use synonyms.\n'
# In[21]:
index = faiss.IndexFlatL2(len(embd[0])) 
index.add(np.array(embd))
# In[10]:

# In[ ]: ques_ind->(ques,(text, indices)) ans_ext_embd->(ques, pred, embeddings) In[ ]:
def generate_indices(txt_lst,meta_data,index,txt):
    response = openai.Embedding.create(
      model = 'text-embedding-ada-002',
        input = [txt]
    )
    txt_embd = response['data'][0]['embedding']
    dist, ind = index.search(np.array(txt_embd).reshape(1,-1), 10)
    txts = []
    for i in ind[0]:
        txts.append(txt_lst[i])

    return txts,ind
# In[11]:
def ret_lst_qa(ques_ind, instruct):
    ans_ext_emb = []
    for i in ques_ind:
        prompt = 'Context : """'
        for i in txts:
            prompt+=i[1][0]+"\n"
        prompt += '"""'
        prompt += instruct + 'Question: """' + i[0] + '"""'

        response = openai.Completion.create(
            model='text-davinci-003', 
            prompt=prompt, 
            max_tokens=256,
            temperature=0.3,
            stop=[' END'])

        predt = response["choices"][0]["text"].strip()
        atb = predt.replace("\n",'').split('.')
        embdest = []
        for j in atb:
            if len(j) != 0:
                response = openai.Embedding.create(
                        model = 'text-embedding-ada-002',
                        input = [j]
                )
            embeddings = response['data'][0]['embedding']
            embdest.append(embeddings)
        ans_ext_emb.append((ques_ind[0], predt, embdest))
    return ans_ext_emb

def return_opt_prov(ans_ext_emb,embd,idcs,meta):
    # txt_embd = response.embeddings[0]
    # t1 = embd[ind[0][0]]
    prov_and_conf = []
    for i in ans_ext_emb:
        indcs = idcs
        agg_score = 0
        inpos = dict()
        for j in range(10):
            inpos[j] = 0
        for j in i[2]:
            mxv = -1
            mid = -1
            for k,v in enumerate(indcs[0]):
                t1 = embd[v]
                scr = 1 - spatial.distance.cosine(j,t1)
                if scr>mxv:
                    mxv = scr
                    mid = k
            inpos[mid]+=1
            agg_score += mxv
        indpt = []
        indpt.append(predt)
        ppm = 0
        prov = []
        for k,v in inpos.items():
            if v!=0:
                ppm+=1
                pt = meta[indcs[0][k]]
                pt['percent'] = (v/10)*100
                prov.append(pt)
        stn = ''
        if agg_score/2*ppm>0.6:
            stn = 'High'
        elif 0.5<agg_score/ppm and agg_score/ppm<0.7:
            stn = 'Medium'
        else:
            stn = 'Weak'
        indpt.append((predt, stn, prov))
        prov_and_conf.append((indpt, agg_score/2*ppm))
    return prov_and_conf
# In[12]:
CONNECTION_STRING = 'mongodb+srv://FullStackAdmin:Hello%4012345@mongo-rfp-sandbox-eus-001.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000'
# In[14]:
DB_NAME = "foundationalai" 
COLLECTION_NAME = "documents"
# Create database if it doesn't
client = pymongo.MongoClient(CONNECTION_STRING) 
db = client[DB_NAME] 
collection = db[COLLECTION_NAME] 
docs = db.documents
# In[ ]: get mongoID through fastapi: read documentation In[22]:
def main_gen(MongoID, text_lst, meta_lst, embd_lst, index):
    email_bson = docs.find_one({"_id":MongoID})
    email_json = json.loads(email_bson)
    ans_ext_id = []
    for i in email_bson["Mail"]["Message"]:
        mID = i["MessageId"]
        m_body = str(i["MailBody"])
        ls = gen_ques(m_body)
        ques_indcs = []
        for j in ls:
            text, ind = generate_indices(text_lst,meta_lst,embd_lst,j,index)
            txt_indcs.append(j, (text, ind))
        instruct = 'You are a sales employee at LevelAccess. From the above given context answer the below question from a potential customer to the best of your knowledge. The answer should strictly be from the context and if you dont know or are unsure about it then just say "I Dont Know". Only refer to levelaccess when you answer the question and not Essential Accessibility. Assume that by "you" or "your" in the question, they mean LevelAccess. Also pay special attention to any corporation names that are mentioned and only include them in the answer if they are present in the contect. Follow the question wording strictly and donot use synonyms.\n'
        ans_ext = ret_lst_qa(ques_indcs, instruct)
        ans_prv_cnf = return_opt_prov(ans_ext,embd_lst,index,meta_lst)
        dic = {}
        for i in len(ques_indcs):
            dic[ques_indcs[0]] = {"Answer": ans_prv_cnf[i][0], "ConfidenceScore": ans_prv_cnf[i][1], "References": ans_prv_cnf[i][2]}
        #ans_ext_id.append((mID, dic))
        i["DictQnA"] = dic
    email_bson = BSON.encode(email_json)
    doc_id = docs.insert_one(email_bson).inserted_id
    return doc_id

@app.get('/m2_endpt/')