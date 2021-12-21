import os
import pprint
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import OperationFailure

MONGODB_URL = os.getenv('MONGODB_URL', default='mongodb://admin:supersecret@127.0.0.1:27017/?authSource=admin')

def get_mongo(conn_str, db):
    return MongoClient(conn_str)[db]

def highway_view_options(name, direction):
    return [
        {
            '$match': {
                'highwayname': name,
                'shortdirection': direction,
            },
        },
        { '$unset': 'stationlist.detectorlist' },
        { '$unwind': '$stationlist' },
    ]

if __name__ == '__main__':
    db = get_mongo(MONGODB_URL, 'freeway')
    try:
        db.command({
            'create': 'highway_i205_n',
            'viewOn': 'highway',
            'pipeline': highway_view_options('I-205', 'N'),
        })
    except OperationFailure as exc:
        if exc.code != 48:
            raise exc
    # 1. Get initial station
    starting_station = 'Johnson Cr NB'
    ending_station = 'Columbia to I-205 NB'
    highway = list(db.highway_i205_n.aggregate([
        { '$match': { 'stationlist.locationtext': 'Johnson Cr NB' } },
    ]))
    route = []
    # 2. Iterate through next stations
    i = 0
    while i < 1000:
        station_info = highway[0]['stationlist']
        station_id = station_info.get('downstream', '0')
        station_name = station_info.get('locationtext', '')
        route.append(station_info)
        if station_name == ending_station:
            break
        if i >= 1000 or station_id == '0':
            route = []
            break
        highway = list(db.highway_i205_n.aggregate([
            { '$match': { 'stationlist.stationid': station_id } },
        ]))
        i += 1
    pprint.pprint(route)

# Result:
#[{'downstream': '1047',
#  'highwayid': '3',
#  'latlon': '45.45322,-122.572585',
#  'length': '1.89',
#  'locationtext': 'Johnson Cr NB',
#  'milepost': '16.2',
#  'numberlanes': '3',
#  'stationclass': '1',
#  'stationid': '1046',
#  'upstream': '1045'},
# {'downstream': '1117',
#  'highwayid': '3',
#  'latlon': '45.478984,-122.565617',
#  'length': '1.6',
#  'locationtext': 'Foster NB',
#  'milepost': '18.1',
#  'numberlanes': '3',
#  'stationclass': '1',
#  'stationid': '1047',
#  'upstream': '1046'},
# {'downstream': '1048',
#  'highwayid': '3',
#  'latlon': '45.497415,-122.565244',
#  'length': '0.84',
#  'locationtext': 'Powell to I-205 NB',
#  'milepost': '19.4',
#  'numberlanes': '3',
#  'stationclass': '1',
#  'stationid': '1117',
#  'upstream': '1047'},
# {'downstream': '1142',
#  'highwayid': '3',
#  'latlon': '45.504373,-122.565148',
#  'length': '0.86',
#  'locationtext': 'Division NB',
#  'milepost': '19.78',
#  'numberlanes': '3',
#  'stationclass': '1',
#  'stationid': '1048',
#  'upstream': '1117'},
# {'downstream': '1140',
#  'highwayid': '3',
#  'latlon': '45.526397,-122.565153',
#  'length': '1.82',
#  'locationtext': 'Glisan to I-205 NB',
#  'milepost': '21.12',
#  'numberlanes': '3',
#  'stationclass': '1',
#  'stationid': '1142',
#  'upstream': '1048'},
# {'downstream': '0',
#  'highwayid': '3',
#  'latlon': '45.559917,-122.564161',
#  'length': '2.14',
#  'locationtext': 'Columbia to I-205 NB',
#  'milepost': '23.41',
#  'numberlanes': '3',
#  'stationclass': '1',
#  'stationid': '1140',
#  'upstream': '1142'}]
