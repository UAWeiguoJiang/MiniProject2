import json
from pymongo import MongoClient

port_number = int(input('Server port number: '))
client = MongoClient("localhost", port_number)
db = client['291db']

name_basics = db['name_basics']
title_basics = db['title_basics']
title_principals = db['title_principals']
title_ratings = db['title_ratings']

def drop_collections():
    name_basics.drop()
    title_basics.drop()
    title_principals.drop()
    title_ratings.drop()

    return


def create_collections():
    drop_collections()

    json2mongo('name.basics.json', name_basics)
    json2mongo('title.basics.json', title_basics)
    json2mongo('title.principals.json', title_principals)
    json2mongo('title.ratings.json', title_ratings)

    return


def json2mongo(input_file, collection):
    with open(input_file) as f:
        file_data = json.load(f)
        collection.insert_many(file_data)

    return


def main():
    create_collections()
    for i in client.list_databases():
    	print(i)


if __name__ == "__main__":
    main()
