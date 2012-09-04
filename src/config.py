#!/usr/bin/python
# -*- coding:utf-8 -*-

import logging
import pymongo

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
RRA = [(1, '1d', 86400), (3, '1w', 604800), (15, '1m', 2592000), (30, '1y', 30758400)]
#数据库名称（站点名称）配置
CLUSTER = 'vdc'
CATEGORY = ['host', 'vm', 'switch', 'flow', 'ext', 'cluster']
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

#sFlow相关配置
SFLOWTOOL = '/usr/local/bin/sflowtool'