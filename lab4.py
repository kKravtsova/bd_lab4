import pandas as pd
from pymongo import MongoClient, ASCENDING, errors
import json

FILL = False
SHOWBEST = True
CSVPATHONE = 'Odata2019File.csv'
CSVPATHTWO = 'Odata2020File.csv'
DBNAME = 'lab4'
COLLECTION = 'students'

def mongoimport(csv_path, db_name, coll_name, db_url='localhost', db_port=27017):
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[coll_name]
    coll.create_index([('OUTID', ASCENDING)],unique=True)
    data = pd.read_csv(csv_path, sep=';', encoding='cp1251', decimal=',')
    payload = json.loads(data.to_json(orient='records'))
    for student in payload:
        try:
            coll.insert_one(student)
        except errors.DuplicateKeyError:
            print("Skiped")

if(FILL):
    mongoimport(CSVPATHONE, DBNAME, COLLECTION)
    mongoimport(CSVPATHTWO, DBNAME, COLLECTION)

def findBest(csv_path, db_name, coll_name, db_url='localhost', db_port=27017):
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[coll_name]
    regions = coll.find().distinct('REGNAME')
    for region in regions:
        best = coll.find({'REGNAME': region, 'UkrTestStatus': 'Зараховано'}).sort('UkrBall100', -1).limit(1)
        print(best[0]['OUTID'], best[0]['REGNAME'], best[0]['UkrBall100'])


if(SHOWBEST):
    findBest(CSVPATHONE, DBNAME, COLLECTION)