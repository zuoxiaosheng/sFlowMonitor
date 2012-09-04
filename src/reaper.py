#!/usr/bin/python
# -*- coding:utf-8 -*-

import time, sched
from config import db, RRA, CATEGORY

now = int(time.time())

#保证监控数据的固定时间长度
def data_fixed_length():
	for i in CATEGORY:
		db[i].remove({'unixSecondsUTC': {"$lt": now-24*60*60}})
		if i != 'flow':
			for item in RRA:
				cname = i + '_' + str(item[0]) + '_' + item[1]
				length = item[2]
				db[cname].remove({'unixSecondsUTC': {"$lt": now-length}})

def perform(s, inc, func, args):
    s.enter(inc,0,perform,(s, inc, func, args))
    func(**args)
   
def schedule(s, inc, func, args):
    s.enter(0,0,perform,(s, inc, func, args))
    s.run()

if __name__ == '__main__':
	scheduler = sched.scheduler(time.time,time.sleep)
	schedule(scheduler, 60*60, data_fixed_length, {})