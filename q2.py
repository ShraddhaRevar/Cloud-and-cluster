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

def get_volume_aggregate(detector_id_list):
    return [
	{
	    '$match': {
		'$and': [
		    {
			'$or': list(map(lambda d: { 'detectorid': d['detectorid'] }, detector_id_list)),
		    },
		    {
			'starttime': {
                            '$gte': datetime(2011, 9, 15, 0, 0, 0, 0, timezone(timedelta(hours=-7))),
                            '$lt':  datetime(2011, 9, 16, 0, 0, 0, 0, timezone(timedelta(hours=-7))),
			},
		    },
		]
	    },
	},
	{
	    '$group': {
		'_id': 1,
		'total_volume': { '$sum': '$volume' },
	    },
	},
    ]


if __name__ == '__main__':
    db = get_mongo(MONGODB_URL, 'freeway')
    # 1. Get detectors for Foster NB station
    station_list = list(db.highway.aggregate(get_station_info_aggregate('Foster NB')))
    pprint.pprint(station_list)
    # 2. Compute volume for the detectors
    detector_list = station_list[0]['detectorlist'] if len(station_list) > 0 else []
    volume_info = db.loopdata.aggregate(get_volume_aggregate(detector_list))
    pprint.pprint(list(volume_info))


# Result:
#[{'_id': 1, 'total_volume': 49891}]
