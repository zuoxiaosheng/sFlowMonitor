#!/usr/bin/python
# -*- coding:utf-8 -*-

import time
from config import db, RRA, CATEGORY

#保证监控数据的固定时间长度
def reap():
	now = int(time.time())
	for i in CATEGORY:
		db[i].remove({'unixSecondsUTC': {"$lt": now-24*60*60}})
		if i != 'flow':
			for item in RRA:
				cname = i + '_' + str(item[0]) + '_' + item[1]
				length = item[2]
				db[cname].remove({'unixSecondsUTC': {"$lt": now-length}})