import os
from pymongo import MongoClient

MONGODB_URL = os.getenv('MONGODB_URL', default='mongodb://admin:supersecret@127.0.0.1:27017/?authSource=admin')

def get_mongo(conn_str, db):
    return MongoClient(conn_str)[db]

if __name__ == '__main__':
    db = get_mongo(MONGODB_URL, 'freeway')
    speed_count = db.loopdata.aggregate([
	{
	    '$match': { '$and': [
		{ 'speed': { '$ne': 0 } },
		{ '$or': [ {'speed': {'$lt': 5}}, {'speed': {'$gt': 80}} ] },
	    ]},
	},
	{
	    '$count': 'speed_count',
	},
    ])
    print(list(speed_count))


# Result:
#[{'speed_count': 130182}]
