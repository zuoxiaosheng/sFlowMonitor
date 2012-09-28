#!/usr/bin/python
# -*- coding:utf-8 -*-

import pymongo
from config import RRA, db

def init():
	db['host'].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('UUID', pymongo.ASCENDING)])
	db['host'].ensure_index([('UUID', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
	for item in RRA:
		cname = 'host_' + str(item[0]) + '_' + item[1]
		db[cname].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('UUID', pymongo.ASCENDING)])
		db[cname].ensure_index([('UUID', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])

	db['vm'].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('UUID', pymongo.ASCENDING)])
	db['vm'].ensure_index([('UUID', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
	for item in RRA:
		cname = 'vm_' + str(item[0]) + '_' + item[1]
		db[cname].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('UUID', pymongo.ASCENDING)])
		db[cname].ensure_index([('UUID', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])

	db['switch'].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('agent', pymongo.ASCENDING), ('ifIndex', pymongo.ASCENDING)])
	db['switch'].ensure_index([('agent', pymongo.ASCENDING), ('ifIndex', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
	for item in RRA:
		cname = 'switch_' + str(item[0]) + '_' + item[1]
		db[cname].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('agent', pymongo.ASCENDING), ('ifIndex', pymongo.ASCENDING)])
		db[cname].ensure_index([('agent', pymongo.ASCENDING), ('ifIndex', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])

	db['ext'].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('agent', pymongo.ASCENDING)])
	db['ext'].ensure_index([('agent', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
	for item in RRA:
		cname = 'ext_' + str(item[0]) + '_' + item[1]
		db[cname].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('agent', pymongo.ASCENDING)])
		db[cname].ensure_index([('agent', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
	
	for item in RRA:
		cname = 'cluster_' + str(item[0]) + '_' + item[1]
		db[cname].ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('UUID', pymongo.ASCENDING)])
		db[cname].ensure_index([('UUID', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
