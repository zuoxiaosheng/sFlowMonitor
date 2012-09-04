#!/usr/bin/python
# -*- coding:utf-8 -*-

import pymongo
import logging

#日志
logfile = 'collector.log'
def initlog():
        logger = logging.getLogger()
        handler = logging.FileHandler(logfile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.NOTSET)
        return logger

logger = initlog()

#MongoDB基本配置
MONGO_IP = '127.0.0.1'
MONGO_PORT = 27017
#多分辨率数据配置
STEP = 20
RRA = [(1, 86400, '1d'), (3, 604800, '1w'), (15, 2592000, '1m'), (30, 30758400, '1y')]
#数据库名称（站点名称）配置
CLUSTER = 'vdc'
#监控数据定义
METRICS = {
	'common': {
		'unixSecondsUTC': { 'dst': 'GAUGE', 'group': 'system', 'type': 'int' },
		'agent': { 'dst': 'GAUGE', 'group': 'system', 'type': 'str' },
		'sysUpTime': { 'dst': 'GAUGE', 'group': 'system', 'type': 'int' },
		'samplesInPacket': { 'dst': 'GAUGE', 'group': 'system', 'type': 'int' },
	},
	'host': {
		'disk_total': { 'dst': 'GAUGE', 'group': 'disk', 'type': 'float' },
		'disk_free': { 'dst': 'GAUGE', 'group': 'disk', 'type': 'float' },
		'disk_partition_max_used': { 'dst': 'GAUGE', 'group': 'disk', 'type': 'float' },
		'disk_reads': { 'dst': 'COUNTER', 'group': 'disk', 'type': 'float' },
		'disk_bytes_read': { 'dst': 'COUNTER', 'group': 'disk', 'type': 'float' },
		'disk_read_time': { 'dst': 'COUNTER', 'group': 'disk', 'type': 'float' },
		'disk_writes': { 'dst': 'COUNTER', 'group': 'disk', 'type': 'float' },
		'disk_bytes_written': { 'dst': 'COUNTER', 'group': 'disk', 'type': 'float' },
		'disk_write_time': { 'dst': 'COUNTER', 'group': 'disk', 'type': 'float' },

		'mem_total': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'mem_free': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'mem_shared': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'mem_buffers': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'mem_cached': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'swap_total': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'swap_free': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'page_in': { 'dst': 'COUNTER', 'group': 'memory', 'type': 'float' },
		'page_out': { 'dst': 'COUNTER', 'group': 'memory', 'type': 'float' },
		'swap_in': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },
		'swap_out': { 'dst': 'GAUGE', 'group': 'memory', 'type': 'float' },

		'cpu_load_one': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_load_five': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_load_fifteen': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_proc_run': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'int' },
		'cpu_proc_total': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'int' },
		'cpu_num': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'int' },
		'cpu_speed': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'int' },
		'cpu_uptime': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'int' },
		'cpu_user': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_nice': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_system': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_idle': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_wio': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpuintr': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpu_sintr': { 'dst': 'GAUGE', 'group': 'cpu', 'type': 'float' },
		'cpuinterrupts': { 'dst': 'COUNTER', 'group': 'cpu', 'type': 'float' },
		'cpu_contexts': { 'dst': 'COUNTER', 'group': 'cpu', 'type': 'float' },

		'nio_bytes_in': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },
		'nio_pkts_in': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },
		'nio_errs_in': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },
		'nio_drops_in': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },
		'nio_bytes_out': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },
		'nio_pkts_out': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },
		'nio_errs_out': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },
		'nio_drops_out': { 'dst': 'COUNTER', 'group': 'network', 'type': 'float' },

		'hostname': { 'dst': 'GAUGE', 'group': 'system', 'type': 'str' },
		'UUID': { 'dst': 'GAUGE', 'group': 'system', 'type': 'str' },
		'machine_type': { 'dst': 'GAUGE', 'group': 'system', 'type': 'str' },
		'os_name': { 'dst': 'GAUGE', 'group': 'system', 'type': 'str' },
		'os_release': { 'dst': 'GAUGE', 'group': 'system', 'type': 'str' },
	},
	'vm': {
		'vdsk_capacity': { 'dst': 'GAUGE', 'group': 'vdisk', 'type': 'float' },
		'vdsk_allocation': { 'dst': 'GAUGE', 'group': 'vdisk', 'type': 'float' },
		'vdsk_available': { 'dst': 'GAUGE', 'group': 'vdisk', 'type': 'float' },
		'vdsk_rd_req': { 'dst': 'COUNTER', 'group': 'vdisk', 'type': 'float' },
		'vdsk_rd_bytes': { 'dst': 'COUNTER', 'group': 'vdisk', 'type': 'float' },
		'vdsk_wr_req': { 'dst': 'COUNTER', 'group': 'vdisk', 'type': 'float' },
		'vdsk_wr_bytes': { 'dst': 'COUNTER', 'group': 'vdisk', 'type': 'float' },
		'vdsk_errs': { 'dst': 'COUNTER', 'group': 'vdisk', 'type': 'float' },

		'vmem_memory': { 'dst': 'GAUGE', 'group': 'vmemory', 'type': 'float' },
		'vmem_maxMemory': { 'dst': 'GAUGE', 'group': 'vmemory', 'type': 'float' },

		'vcpu_state': { 'dst': 'GAUGE', 'group': 'vcpu', 'type': 'str' },
		'vcpu_cpu_mS': { 'dst': 'GAUGE', 'group': 'vcpu', 'type': 'float' },
		'vcpu_cpuCount': { 'dst': 'GAUGE', 'group': 'vcpu', 'type': 'float' },

		'vnio_bytes_in': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },
		'vnio_pkts_in': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },
		'vnio_errs_in': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },
		'vnio_drops_in': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },
		'vnio_bytes_out': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },
		'vnio_pkts_out': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },
		'vnio_errs_out': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },
		'vnio_drops_out': { 'dst': 'COUNTER', 'group': 'vnetwork', 'type': 'float' },

		'hostname': { 'dst': 'GAUGE', 'group': 'vsystem', 'type': 'str' },
		'UUID': { 'dst': 'GAUGE', 'group': 'vsystem', 'type': 'str' },
		'machine_type': { 'dst': 'GAUGE', 'group': 'vsystem', 'type': 'str' },
		'os_name': { 'dst': 'GAUGE', 'group': 'vsystem', 'type': 'str' },
	},
	'switch': {
		'ifIndex': { 'dst': 'GAUGE', 'group': 'switch', 'type': 'str' },
		'networkType': { 'dst': 'GAUGE', 'group': 'switch', 'type': 'str' },
		'ifSpeed': { 'dst': 'GAUGE', 'group': 'switch', 'type': 'int' },
		'ifDirection': { 'dst': 'GAUGE', 'group': 'switch', 'type': 'str' },
		'ifStatus': { 'dst': 'GAUGE', 'group': 'switch', 'type': 'str' },
		'ifInOctets': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifInUcastPkts': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifInMulticastPkts': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifInBroadcastPkts': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifInDiscards': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifInErrors': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifInUnknownProtos': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifOutOctets': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifOutUcastPkts': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifOutMulticastPkts': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifOutBroadcastPkts': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifOutDiscards': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifOutErrors': { 'dst': 'COUNTER', 'group': 'switch', 'type': 'float' },
		'ifPromiscuousMode': { 'dst': 'GAUGE', 'group': 'switch', 'type': 'str' },
	},
	'flow': {
		'meanSkipCount': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'int' },
		'inputPort': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'outputPort': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'extendedType': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'in_vlan': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'in_priority': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'out_vlan': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'flowSampleType': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'headerProtocol': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
		'sampledPacketSize': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'int' },
		'strippedBytes': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'int' },
		'headerLen': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'int' },
		'headerBytes': { 'dst': 'GAUGE', 'group': 'flow', 'type': 'str' },
	},
	'ext': {
		'+1d5V': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'+3d3V': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'+5V': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'+12V': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'+3d3VSB': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'VBAT': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'CPU1_Vcore': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'CPU2_Vcore': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'CPU1_DIMM': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'CPU2_DIMM': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'CPU1DIMM': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'CPU2DIMM': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'System_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P1-DIMM1A_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P1-DIMM1B_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P1-DIMM2A_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P1-DIMM2B_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P1-DIMM3A_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P1-DIMM3B_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P2-DIMM1A_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P2-DIMM1B_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P2-DIMM2A_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P2-DIMM2B_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P2-DIMM3A_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'P2-DIMM3B_Temp': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'Fan1': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'Fan2': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'Fan3': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'Fan4': { 'dst': 'GAUGE', 'group': 'ipmi', 'type': 'float' },
		'ping': { 'dst': 'GAUGE', 'group': 'other', 'type': 'float' },
	}
}


#数据库相关初始化
connection = pymongo.Connection(MONGO_IP, MONGO_PORT)
db = connection[CLUSTER]
host = db.host
vm = db.vm
switch = db.switch
flow = db.flow
host.ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('agent', pymongo.ASCENDING)])
host.ensure_index([('agent', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
vm.ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('agent', pymongo.ASCENDING), ('UUID', pymongo.ASCENDING)])
vm.ensure_index([('agent', pymongo.ASCENDING), ('UUID', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])
switch.ensure_index([('unixSecondsUTC', pymongo.ASCENDING), ('agent', pymongo.ASCENDING), ('ifIndex', pymongo.ASCENDING)])
switch.ensure_index([('agent', pymongo.ASCENDING), ('ifIndex', pymongo.ASCENDING), ('unixSecondsUTC', pymongo.ASCENDING)])

#计量数据集合
meter = db.meter

#扩展监控数据集合
ext = db.ext


#将监控数据转换为适当的数据类型
def data_convert(category, key, data):
	if METRICS[category][key]['type'] == 'float':
		return float(data)
	elif METRICS[category][key]['type'] == 'int':
		return int(data)
	else:
		return data

#处理采样时间间隔的波动
def time_uniform(category, condition, time):
	logger.info('uniform time slot')
	document = db[category].find_one(condition, sort=[('unixSecondsUTC', -1)])
	time = document['unixSecondsUTC'] + int(round((time - document['unixSecondsUTC'])/(float(STEP))))*STEP
	return time

#保证监控数据的固定时间长度
def data_fixed_length(cname, time, length):
	logger.info('fix time period')
	db[cname].remove({'unixSecondsUTC': {"$lt": time-length}})

#处理并存储计量数据
def meter_process(data):
	logger.info('process metering data')
	meter_data = {'vm': data['hostname'], 'uuid': data['UUID'], 
				  'hostIp': data['agent'], 'utc': data['unixSecondsUTC'], 'duration': STEP*30, 'cpuNum': data['vcpu_cpuCount'], 
				  'memTotal': data['vmem_memory'], 'diskTotal': data['vdsk_capacity'], 
				  'bytesIn': data['vnio_bytes_in']*STEP*30, 'bytesOut': data['vnio_bytes_out']*STEP*30}
	meter.insert(meter_data)

#更新某个集合
def collection_update(category, cname, length, last_n):
	logger.info('update collection')
	count = last_n.count()
	ave = last_n.next()
	ave.pop('_id')
	i = 1
	for item in last_n:
		i += 1
		for key in item:
			if METRICS[category].has_key(key) and METRICS[category][key]['type'] != 'str':
				ave[key] += item[key]
				if i == count:
					if ave[key] >= 0:
						ave[key] /= count
					else:
						ave[key] = 0
	db[cname].insert(ave)
	data_fixed_length(cname, ave['unixSecondsUTC'], length)
	if category == 'vm' and cname.split('_')[1] == '30':
		meter_process(ave)

#聚合数据为更大的时间间隔，更长的时间段
def data_aggregate(category, data, condition):
	logger.info('aggregate data')
	collection_1 = db[category+'_1_1d']
	for item in RRA[1:]:
		cname = category + '_' + str(item[0]) + '_' + item[2]
		collection_n = db[cname]
		count = item[0]
		if collection_n.find(condition).count():
			last = collection_n.find_one(condition, sort=[('unixSecondsUTC', -1)])
			if data['unixSecondsUTC'] >= (last['unixSecondsUTC'] + (2*count-1)*STEP):
				last_n = collection_1.find(dict(condition, **{'unixSecondsUTC': {"$gt": data['unixSecondsUTC']-count*STEP, "$lte": data['unixSecondsUTC']}}))
				#若是cluster的数据要做汇聚，则类别为host，即cluster目前只存host的监控数据
				if category == 'cluster':
					collection_update('host', cname, item[1], last_n)
				else:
					collection_update(category, cname, item[1], last_n)
		else:
			first = collection_1.find_one(condition)
			if data['unixSecondsUTC'] >= (first['unixSecondsUTC'] + (count-1)*STEP):
				last_n = collection_1.find(dict(condition, **{'unixSecondsUTC': {"$lt": first['unixSecondsUTC'] + count*STEP}}))
				if category == 'cluster':
					collection_update('host', cname, item[1], last_n)
				else:
					collection_update(category, cname, item[1], last_n)

#更新一个STEP间隔的cluster监控数据集合
def cluster_process(collection, last_n, utc):
	logger.info('process cluster data')
	if last_n.count():
		n = last_n.count()
		sum = last_n.next()
		sum.pop('_id')
		sum['unixSecondsUTC'] = utc
		for key in sum.keys():
			if key == 'unixSecondsUTC':
				continue
			if  (not METRICS['host'].has_key(key)) or METRICS['host'][key]['group'] == 'system':
				sum.pop(key)
		for item in last_n:
			for key in sum:
			    if key == 'unixSecondsUTC':
			    	continue
			    sum[key] += item[key]
		for key in ['cpu_user', 'cpu_nice', 'cpu_system', 'cpu_idle', 
					'cpu_wio', 'cpuintr', 'cpu_sintr']:
			sum[key] /= n
		collection.insert(sum)
		return sum
	else:
		return 0

#更新cluster集合中的监控数据
def cluster_update(data):
	logger.info('insert cluster data')
	cluster = db['cluster_1_1d']
	host = db['host_1_1d']
	if cluster.count():
		cluster_last = cluster.find_one(sort=[('unixSecondsUTC', -1)])
		if data['unixSecondsUTC'] >= cluster_last['unixSecondsUTC']+STEP:
			host_last_n = host.find({'unixSecondsUTC': {"$gte": data['unixSecondsUTC']-STEP, "$lt": data['unixSecondsUTC']}}, sort=[('unixSecondsUTC', -1)])
			sum = cluster_process(cluster, host_last_n, data['unixSecondsUTC'])
			if sum:
				data_aggregate('cluster', sum, {})
	else:
		host_first = host.find_one({'hostname': data['hostname']})
		if data['unixSecondsUTC'] - host_first['unixSecondsUTC'] >= STEP:
			host_last_n = host.find({'unixSecondsUTC': {"$gte": data['unixSecondsUTC']-STEP, "$lt": data['unixSecondsUTC']}}, sort=[('unixSecondsUTC', -1)])
			cluster_process(cluster, host_last_n, data['unixSecondsUTC'])

#计算物理机和虚机的CPU各项使用率
def cpu_util(category, data, document):
	logger.info('calculate cpu util')
	if category == 'host':
		cpu_metrics = ['cpu_user', 'cpu_nice', 'cpu_system', 'cpu_idle', 
					   'cpu_wio', 'cpuintr', 'cpu_sintr']
		cpu_total = 0
		for metric in cpu_metrics:
			cpu_total += data[metric] - document[metric]
		for metric in cpu_metrics:
			data[metric] = round((data[metric] - document[metric])/cpu_total*100, 2)
			if data[metric] <0:
				data[metric] == 0
		return data
	else:
		data['vcpu_cpu_mS'] = round((data['vcpu_cpu_mS'] - document['vcpu_cpu_mS'])/((data['unixSecondsUTC'] - document['unixSecondsUTC'])*1000)*100, 2)
		if data['vcpu_cpu_mS'] < 0:
			data['vcpu_cpu_mS'] = 0
		return data

#处理COUNTER（累增）型的数据并插入数据库，调用数据聚合函数以及cluster更新函数
def data_process(category, data, condition):
	logger.info('process data')
	collection = db[category+'_1_1d']
	document = db[category].find(condition, sort=[('unixSecondsUTC', -1)]).limit(2)[1]
	#处理COUNTER类型数据，即（当前收到的数据-数据库中最新的数据）/步长
	for key in data:
		if METRICS[category].has_key(key) and METRICS[category][key]['dst'] == 'COUNTER' :
			data[key] = round((data[key] - document[key])/STEP, 2)
			if data[key] < 0:
				data[key] = 0
	#如果是host或vm，则还需要将各种CPU时间转换成利用率
	if category in ['host', 'vm']:
		data = cpu_util(category, data, document)
	collection.insert(data)
	data_fixed_length(category+'_1_1d', data['unixSecondsUTC'], 86400)
	#处理完COUNTER数据后，对数据进行汇聚
	data_aggregate(category, data, condition)
	#汇聚完数据后，更新cluster信息
	if category == 'host':
		cluster_update(data)

#将sFlow采样的监控数据插入数据库相应的集合中，并调用处理COUNTER型数据的函数
def data_insert(header, data):
	logger.info('insert data')
	data['unixSecondsUTC'] = header['unixSecondsUTC']
	data['agent'] = header['agent']
	#每种数据放到对应的集合中
	if data.has_key('meanSkipCount'):
		flow.insert(data)
	else:
		if data.has_key('disk_total'):
			category = 'host'
			condition = {'agent': data['agent']}
		if data.has_key('vdsk_capacity'):
			category = 'vm'
			condition = {'agent': data['agent'], 'UUID': data['UUID']}
		if data.has_key('ifIndex'):
			category = 'switch'
			condition = {'agent': data['agent'], 'ifIndex': data['ifIndex']}
		document = db[category].find_one(condition, sort=[('unixSecondsUTC', -1)])
		if document == None:
			db[category].insert(data)
		#保证不插入重复的时间戳
		else:
			#时间归一化处理，保证样本之间时间间隔一致
                        data['unixSecondsUTC'] = time_uniform(category, condition, data['unixSecondsUTC'])
			if document['unixSecondsUTC'] < data['unixSecondsUTC']:
				db[category].insert(data)
				#保证原始数据长度为一天
				data_fixed_length(category, data['unixSecondsUTC'], 86400)
				#当该节点（host、vm、switch端口）监控数据的文档数为两条以上时，开始处理COUNTER型数据
				data_process(category, data, condition)

#扩展监控数据插入数据库
def ext_data_insert(data):
	category = 'ext'
	condition = {'ip': data['ip']}
	document = ext.find_one(condition, sort=[('unixSecondsUTC', -1)])
	if document == None:
		ext.insert(data)
	elif document['unixSecondsUTC'] != data['unixSecondsUTC']:
		data['unixSecondsUTC'] = time_uniform(category, condition, data['unixSecondsUTC'])
		ext.insert(data)
		data_fixed_length(category, data['unixSecondsUTC'], 86400)
		data_process(category, data, condition)
