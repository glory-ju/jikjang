import pymongo

def set_mongodb(hostname, port, username, password, db_name):
    # DB 접속
    conn = pymongo.MongoClient(f'mongodb://{username}:{password}@{hostname}:{port}/')
    # DB 객체 할당, 없으면 자동 생성됨
    return conn[db_name]

def mongo_insert(data, collection_name, db):
    collection = db[str(collection_name)]
    collection.insert_one(data)