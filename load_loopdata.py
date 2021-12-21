import os
import pymongo
from datetime import datetime

MONGO_CONN_STR = os.getenv("MONGO_CONN_STR", "mongodb://admin:supersecret@127.0.0.1:27017/freeway?authSource=admin")
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S%z'

def _mongo_client(conn_str):
    from pymongo import MongoClient
    client = MongoClient(conn_str)
    def decorator(fn):
        def wrapper(*args, **kwargs):
            return fn(*args, client=client, **kwargs)
        return wrapper
    return decorator

@_mongo_client(MONGO_CONN_STR)
def _insert_loopdata(rows, client=None):
    db = client.freeway
    loopdata = db.loopdata
    rows = [_transform_loopdata(row) for row in rows]
    try:
        loopdata.insert_many(
            rows,
            ordered=False,
        )
    except pymongo.errors.DuplicateKeyError:
        pass

def csv_read(filename, callback):
    import csv
    with open(filename) as f:
        r = csv.DictReader(f, delimiter=',', restval='')
        done = False
        while not done:
            rows = []
            for i, row in enumerate(r):
                rows.append(row)
                if i > 100000:
                    break
            else:
                done = True
            callback(rows)

@_mongo_client(MONGO_CONN_STR)
def create_indexes(client=None):
    from pymongo import IndexModel, ASCENDING
    db = client.freeway
    loopdata = db.loopdata
    indexes = [
        IndexModel([('starttime', ASCENDING)], unique=False),
        IndexModel([('detectorid', ASCENDING), ('starttime', ASCENDING)], unique=True),
    ]
    loopdata.create_indexes(indexes)

def _transform_loopdata(row):
    detectorid = row['detectorid']
    starttime = row['starttime']
    volume = 0
    speed = 0
    occupancy = 0
    status = 0
    try:
        volume = int(row['volume'])
    except ValueError:
        pass
    try:
        speed = int(row['speed'])
    except ValueError:
        pass
    try:
        occupancy = int(row['occupancy'])
    except ValueError:
        pass
    try:
        status = int(row['status'])
    except ValueError:
        pass
    try:
        starttime = datetime.strptime(row['starttime'], DATETIME_FORMAT)
    except ValueError:
        starttime = datetime.strptime(f"{row['starttime']}00", DATETIME_FORMAT)
    return {
        'detectorid': detectorid,
        'starttime':  starttime,
        'volume':     volume,
        'speed':      speed,
        'occupancy':  occupancy,
        'status':     status,
    }

if __name__ == "__main__":
    create_indexes()
    csv_read('data/freeway_loopdata.csv', _insert_loopdata)
