import os
import pymongo

MONGO_CONN_STR = os.getenv("MONGO_CONN_STR", "mongodb://admin:supersecret@127.0.0.1:27017/freeway?authSource=admin")

def _mongo_client(conn_str):
    from pymongo import MongoClient
    client = MongoClient(conn_str)
    def decorator(fn):
        def wrapper(*args, **kwargs):
            return fn(*args, client=client, **kwargs)
        return wrapper
    return decorator

@_mongo_client(MONGO_CONN_STR)
def _insert_highway(row, client=None):
    db = client.freeway
    highway = db.highway
    highway.update_one(
        {'highwayid': row['highwayid']},
        {'$set': row},
        upsert=True,
    )

@_mongo_client(MONGO_CONN_STR)
def _insert_station(row, client=None):
    db = client.freeway
    highway = db.highway
    highway.update_one(
        {'highwayid': row['highwayid']},
        {'$addToSet': {'stationlist': row}},
        upsert=True,
    )

@_mongo_client(MONGO_CONN_STR)
def _insert_detector(row, client=None):
    db = client.freeway
    highway = db.highway
    try:
        highway.update_one(
            {'highwayid': row['highwayid']},
            {'$addToSet': {'stationlist.$[elem].detectorlist': row}},
            upsert=True,
            array_filters=[
                {'elem.stationid': row['stationid']},
            ],
        )
    except pymongo.errors.DuplicateKeyError:
        pass

@_mongo_client(MONGO_CONN_STR)
def create_indexes(client=None):
    from pymongo import IndexModel, ASCENDING
    db = client.freeway
    highway = db.highway
    indexes = [
        IndexModel([('highwayid', ASCENDING)], unique=True),
        IndexModel([('highwayid', ASCENDING), ('stationlist.stationid', ASCENDING)], unique=True),
        IndexModel([
            ('highwayid', ASCENDING),
            ('stationlist.stationid', ASCENDING),
            ('stationlist.detectorlist.detectorid', ASCENDING)
        ], unique=True),
    ]
    highway.create_indexes(indexes)

def csv_read(filename, callback):
    import csv
    with open(filename) as f:
        r = csv.DictReader(f, delimiter=',', restval='')
        for row in r:
            #print(row)
            callback(row)


if __name__ == "__main__":
    create_indexes()
    csv_read('data/highways.csv', _insert_highway)
    csv_read('data/freeway_stations.csv', _insert_station)
    csv_read('data/freeway_detectors.csv', _insert_detector)
