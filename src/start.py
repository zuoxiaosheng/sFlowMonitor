#!/usr/bin/python
# -*- coding:utf-8 -*-

import time, sched, threading
from config import STEP, RRA
from init import init
from collector import collect
from ext import ext
from processor import process, aggregate, cluster_process, cluster_aggregate
from reaper import reap

def perform(s, inc, func, args):
    s.enter(inc,0,perform,(s, inc, func, args))
    func(**args)
   
def schedule(s, inc, func, args):
    s.enter(0,0,perform,(s, inc, func, args))
    s.run()

if __name__ == '__main__':

	#数据库初始化
	init()

	thread_pool = []

	#增加一个收集线程
	th = threading.Thread(target=collect)
	thread_pool.append(th)
	CATEGORY = [('host',{'UUID':1}), ('vm',{'UUID':1}), ('switch',{'agent':1, 'ifIndex':1}), ('ext', {'agent':1})]

	#增加一个扩展数据收集线程
	scheduler = sched.scheduler(time.time,time.sleep)
	ext_args = {}
	th = threading.Thread(target=schedule, args=(scheduler, 20, ext, ext_args))
	thread_pool.append(th)

	#增加多个处理和汇聚线程
	for i in range(len(CATEGORY)):
		scheduler = sched.scheduler(time.time,time.sleep)
		func_args = {'category':CATEGORY[i][0], 'key':CATEGORY[i][1]}
		th = threading.Thread(target=schedule, args=(scheduler, STEP, process, func_args))
		thread_pool.append(th)
		for j in range(1, len(RRA)):
			scheduler = sched.scheduler(time.time,time.sleep)
			func_args = {'category':CATEGORY[i][0], 'key':CATEGORY[i][1], 'n_step':RRA[j][0], 'time_flag':RRA[j][1]}
			th = threading.Thread(target=schedule, args=(scheduler, STEP*RRA[j][0], aggregate, func_args))
			thread_pool.append(th)	

	#增加多个cluster处理和汇聚线程
	scheduler = sched.scheduler(time.time,time.sleep)
	th = threading.Thread(target=schedule, args=(scheduler, STEP, cluster_process, {}))
	thread_pool.append(th)
	for i in range(1, len(RRA)):
		scheduler = sched.scheduler(time.time,time.sleep)
		func_args = {'n_step':RRA[i][0], 'time_flag':RRA[i][1]}
		th = threading.Thread(target=schedule, args=(scheduler, STEP*RRA[i][0], cluster_aggregate, func_args))
		thread_pool.append(th)

	#增加一个处理定长线程
	scheduler = sched.scheduler(time.time,time.sleep)
	reap_args = {}
	th = threading.Thread(target=schedule, args=(scheduler, 60*60, reap, reap_args))
	thread_pool.append(th)

	#启动所有线程
	for i in range(len(thread_pool)):
		thread_pool[i].start()
