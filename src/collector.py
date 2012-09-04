#!/usr/bin/python
# -*- coding:utf-8 -*-

import subprocess
from config import logger, SFLOWTOOL, METRICS, db

#将监控数据转换为适当的数据类型
def data_convert(category, key, data):
	if METRICS[category][key]['type'] == 'float':
		return float(data)
	elif METRICS[category][key]['type'] == 'int':
		return int(data)
	else:
		return data

#将sFlow采样的监控数据插入数据库相应的集合中
def data_insert(header, data):
	logger.info('insert data')
	data['unixSecondsUTC'] = header['unixSecondsUTC']
	data['agent'] = header['agent']
	#每种数据放到对应的集合中
	if data.has_key('meanSkipCount'):
		logger.info('receive flow data from %s' %(header['agent']))
		category = 'flow'
	if data.has_key('disk_total'):
		logger.info('receive host data from %s:%s' %(header['agent'],item['hostname']))
		category = 'host'
	if data.has_key('vdsk_capacity'):
		logger.info('receive vm data from %s:%s' %(header['agent'],item['hostname']))
		category = 'vm'
	if data.has_key('ifIndex'):
		logger.info('receive switch data from %s' %(header['agent']))
		category = 'switch'
	db[category].insert(data)

if __name__ == '__main__':
	#创建子进程执行sflowtool命令
	sp = subprocess.Popen(SFLOWTOOL, stdout=subprocess.PIPE)
	while True:
		#循环逐行读取标准输出
		line = sp.stdout.readline()
		symbol = line.split()[0]
		if symbol == 'startDatagram':
			header = {}
			samples = []
			i = 0
		elif symbol == 'endDatagram':
			for item in samples:
				#一个sFlow数据包（包含一个头部和一个或多个样本）接收完毕，转数据处理
				data_insert(header, item)
		else:
			if symbol == 'startSample':
				samples.append({})
			elif symbol == 'endSample':
				i += 1
			else:
				[key, value] = line.rstrip('\r\n').split(' ')[:2]
				#if value == 'swift-storage1':
					#print key,value
				for item in METRICS:
					if METRICS[item].has_key(key):
						if item == 'common':
				  			header[key] = data_convert(item, key, value)
				  		else:
				  			samples[i][key] = data_convert(item, key, value)
