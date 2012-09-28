#!/usr/bin/python
# -*- coding:utf-8 -*-

import time
from config import logger, METRICS, STEP, db


#计算物理机的CPU各项使用率
def cpu_util(data, document):
	logger.info('calculate cpu util')
	cpu_metrics = ['cpu_user', 'cpu_nice', 'cpu_system', 'cpu_idle', 
				   'cpu_wio', 'cpuintr', 'cpu_sintr']
	cpu_total = 0
	for metric in cpu_metrics:
		cpu_total += data[metric] - document[metric]
	if cpu_total:
		for metric in cpu_metrics:
			data[metric] = round((data[metric] - document[metric])/cpu_total*100, 2)
			if data[metric] <0:
				data[metric] == 0.00
			elif data[metric] > 100:
				data[metric] = 100.00
	else:
		for metric in cpu_metrics:
			data[metric] = 0.00
	return data

#计算虚机的CPU各项使用率
def vcpu_util(data, document):
	logger.info('calculate vcpu util')
	total = (data['unixSecondsUTC'] - document['unixSecondsUTC'])
	if total:
		data['vcpu_cpu_mS'] = round((data['vcpu_cpu_mS'] - document['vcpu_cpu_mS'])/(total*1000)*100, 2)
		if data['vcpu_cpu_mS'] < 0:
			data['vcpu_cpu_mS'] = 0.00
		elif data['vcpu_cpu_mS'] > 100:
			data['vcpu_cpu_mS'] = 100.00
	else:
		data['vcpu_cpu_mS'] = 0.00
	return data

#将原始数据处理处理成基本数据，包括COUNTER类型数据和CPU数据处理
def process(category, key):
	collection = db[category+'_1_1d']
	now = int(time.time())
	condition = {'unixSecondsUTC': {"$gte": now-2*STEP, "$lt": now}}
	initial = {'unixSecondsUTC': 0}
	reduce = 'function (doc, prev) {'
	reduce += 'if (doc.unixSecondsUTC > prev.unixSecondsUTC) {'
	reduce += 'for (var key in doc) {'
	reduce += 'prev[key] = doc[key]; }}}'
	documents_1 = db[category].group(key, condition, initial, reduce)
	initial = {'unixSecondsUTC': float('inf')}
	reduce = reduce.replace('>', '<')
	documents_2 = db[category].group(key, condition, initial, reduce)
	for i in range(len(documents_1)):
		for key in documents_1[i]:
			if METRICS[category].has_key(key) and METRICS[category][key]['dst'] == 'COUNTER' :
				documents_1[i][key] = round((documents_1[i][key] - documents_2[i][key])/STEP, 2)
				if documents_1[i][key] < 0:
					documents_1[i][key] = 0
		if category == 'host':
			documents_1[i] = cpu_util(documents_1[i], documents_2[i])
		if category == 'vm':
			documents_1[i] = vcpu_util(documents_1[i], documents_2[i])
	if documents_1:
		collection.insert(documents_1)

#将基本数据聚合为更大时间间隔，更长的时间段的数据
def aggregate(category, key, n_step, time_flag):
	collection_1 = db[category+'_1_1d']
	cname = category + '_' + str(n_step) + '_' + time_flag
	collection_n = db[cname]
	count = n_step
	now = int(time.time())
	condition = {'unixSecondsUTC': {"$gte": now-count*STEP, "$lt": now}}
	initial = {'unixSecondsUTC': 0, 'count': 0}
	reduce = 'function (doc, prev) { prev.count++;'
	reduce += 'if (doc.unixSecondsUTC > prev.unixSecondsUTC)'
	reduce += '{prev.unixSecondsUTC = doc.unixSecondsUTC;}'
	reduce += 'for (var key in doc)'
	reduce += '{if (key != "unixSecondsUTC" && key != "_id") '
	reduce += '{if (prev.count == 1) {prev[key] = doc[key];}'
	reduce += 'else {if (typeof(doc[key]) != "string")'
	reduce += '{prev[key] += doc[key];}}}}}'
	finalize = 'function (prev) {for (var key in prev) {'
	finalize += 'if (key != "unixSecondsUTC" && key != "count" && typeof(prev[key]) != "string") {'
	finalize += 'prev[key] /= prev.count;}}delete prev.count;}'
	documents = collection_1.group(key, condition, initial, reduce, finalize)
	if documents:
		collection_n.insert(documents)
		if category == 'vm' and count == 30:
			meter_process(documents)

#更新cluster集合中的监控数据
def cluster_process():
	cluster = db['cluster_1_1d']
	host = db['host_1_1d']
	now = int(time.time())
	key = {'UUID':1}
	condition = {'unixSecondsUTC': {"$gte": now-2*STEP, "$lt": now}}
	initial = {'unixSecondsUTC': 0}
	reduce = 'function (doc, prev) {'
	reduce += 'if (doc.unixSecondsUTC > prev.unixSecondsUTC) {'
	reduce += 'for (var key in doc) {'
	reduce += 'prev[key] = doc[key]; }}}'
	documents = host.group(key, condition, initial, reduce)
	if len(documents):
		num = len(documents)
		sum = documents[0]
		sum.pop('_id')
		sum['unixSecondsUTC'] = now
		for key in sum.keys():
			if key == 'unixSecondsUTC':
				continue
			if  (not METRICS['host'].has_key(key)) or METRICS['host'][key]['group'] == 'system':
				sum.pop(key)
		for item in documents[1:]:
			for key in sum:
			    if key == 'unixSecondsUTC':
			    	continue
			    sum[key] += item[key]
		for key in ['cpu_user', 'cpu_nice', 'cpu_system', 'cpu_idle', 
					'cpu_wio', 'cpuintr', 'cpu_sintr', 'cpu_load_one',
					'cpu_load_five', 'cpu_load_fifteen']:
			sum[key] /= num
		cluster.insert(sum)

#汇聚cluster监控数据
def cluster_aggregate(n_step, time_flag):
	collection_1 = db['cluster_1_1d']
	cname = 'cluster' + '_' + str(n_step) + '_' + time_flag
	collection_n = db[cname]
	num = n_step
	now = int(time.time())
	condition = {'unixSecondsUTC': {"$gte": now-num*STEP, "$lt": now}}
	documents = collection_1.find(condition)
	if documents.count():
		num = documents.count()
		ave = documents.next()
		ave.pop('_id')
		ave['unixSecondsUTC'] = now
		i = 1
		for item in documents:
			i += 1
			for key in item:
				if METRICS['host'].has_key(key) and METRICS['host'][key]['type'] != 'str':
					ave[key] += item[key]
					if i == num:
						if ave[key] >= 0:
							ave[key] /= num
						else:
							ave[key] = 0
		collection_n.insert(ave)

#处理并存储计量数据
def meter_process(data):
	for item in data:
		meter_data = {'vm': item['hostname'], 'uuid': item['UUID'], 
					  'hostIp': item['agent'], 'utc': item['unixSecondsUTC'], 'duration': STEP*30, 'cpuNum': item['vcpu_cpuCount'], 
					  'memTotal': item['vmem_memory'], 'diskTotal': item['vdsk_capacity'], 
					  'bytesIn': item['vnio_bytes_in']*STEP*30, 'bytesOut': item['vnio_bytes_out']*STEP*30}
		db['meter'].insert(meter_data)