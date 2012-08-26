#!/usr/bin/python
# -*- coding:utf-8 -*-

import subprocess
from db import logger, METRICS, data_convert, data_insert


#sFlow相关配置
SFLOWTOOL = '/usr/local/bin/sflowtool'

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
		#if header['agent'] == '10.50.2.11':
		#	print header
		#	print samples
		for item in samples:
			if item.has_key('ifIndex'):
				logger.info('receive switch data from %s' %(header['agent']))
			elif item.has_key('disk_total'):
				logger.info('receive host data from %s:%s' %(header['agent'],item['hostname']))
			elif item.has_key('vdsk_capacity'):
				logger.info('receive vm data from %s:%s' %(header['agent'],item['hostname']))
			else:
				logger.info('receive flow data from %s' %(header['agent']))
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

