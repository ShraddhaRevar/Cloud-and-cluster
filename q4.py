import os
import pprint
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient

MONGODB_URL = os.getenv('MONGODB_URL', default='mongodb://admin:supersecret@127.0.0.1:27017/?authSource=admin')

def get_mongo(conn_str, db):
    return MongoClient(conn_str)[db]

def get_station_info_aggregate(station_name):
    return [
        {
            '$project': {
                'highwayid': 1, 
                'stationlist': {
                    '$filter': {
                        'input': '$stationlist',
                        'as': 'station',
                        'cond': { '$eq': ['$$station.locationtext', station_name] },
                    },
                },
            },
        },
        { '$unwind': '$stationlist' },
        { '$unwind': '$stationlist.detectorlist' },
        {
            '$group': {
                '_id': {
                    'highwayid': '$highwayid',
                    'stationid': '$stationlist.stationid',
                },
                'length': { '$first': { '$toDouble': '$stationlist.length' } },
                'detectorlist': {
                    '$push': '$stationlist.detectorlist',
                },
            },
        },
    ]

def get_average_speed_specific_hours(detector_id_list, distance):
    detectors = list(map(lambda d: { 'detectorid': d['detectorid'] }, detector_id_list))
    return [
        {
            '$match': {
                '$and': [
                    {
                        '$or': detectors,
                    },
                    {
                        'speed': { '$ne': 0 },
                    },
                    {
                        '$or': [
                            { 'starttime': {
                                '$gte': datetime(2011, 9, 22, 7, 0, 0, 0, timezone(timedelta(hours=-7))),
                                '$lt':  datetime(2011, 9, 22, 9, 0, 0, 0, timezone(timedelta(hours=-7))),
                            } },
                            { 'starttime': {
                                '$gte': datetime(2011, 9, 22, 16, 0, 0, 0, timezone(timedelta(hours=-7))),
                                '$lt':  datetime(2011, 9, 22, 18, 0, 0, 0, timezone(timedelta(hours=-7))),
                            } },
                        ],
                    },
                ],
            },
        },
        {
            '$group': {
                '_id': {
                    '$cond': [
                        { '$and': [
                            { '$gte': [ { '$hour': { 'date': '$starttime', 'timezone': '-07:00' } }, 7 ] },
                            { '$lt':  [ { '$hour': { 'date': '$starttime', 'timezone': '-07:00' } }, 9 ] },
                        ] },
                    '07-09',
                    '16-18',
                    ],
                },
            'avg_speed': { '$avg': '$speed' },
            },
        },
        {
            '$addFields': {
                'travel_time': {
                    '$multiply': [
                        { '$divide': [ distance, '$avg_speed' ] },
                        3600,
                    ],
                },
            }
        },
        {
            '$sort': { '_id': 1 },
        },
    ]

if __name__ == '__main__':
    db = get_mongo(MONGODB_URL, 'freeway')
    # 1. Get detectors for Foster NB station
    station_list = list(db.highway.aggregate(get_station_info_aggregate('Foster NB')))
    pprint.pprint(station_list)
    # 2. Compute average speed at specific hour range intervals for the detectors
    detector_list = station_list[0]['detectorlist'] if len(station_list) > 0 else []
    distance = station_list[0]['length'] if len(station_list) > 0 else 0
    average_speed_list = list(db.loopdata.aggregate(get_average_speed_specific_hours(detector_list, distance)))
    pprint.pprint(average_speed_list)

# Result:
#[{'_id': '07-09',
#  'avg_speed': 34.02142857142857,
#  'travel_time': 169.3050598362377},
# {'_id': '16-18',
#  'avg_speed': 50.727586206896554,
#  'travel_time': 113.54768540547889}]
