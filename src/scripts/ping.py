#!/usr/bin/python

import time
import subprocess
import sys

CWD = '/root/Monitor/src/'
sys.path.append(CWD)

PING = '/bin/ping'

for node in open(CWD + 'nodes'):
	[ip, hostname] = node.rstrip('\r\n').split(' ')[:2]
	t0 = time.time()
	timeout = 2
	sp = subprocess.Popen([PING, '-c1', ip], stdout=subprocess.PIPE)
	data = {}
	data['agent'] = ip
	data['hostname'] = hostname
	while time.time() < t0 + timeout:
		if sp.poll() != None:
			for line in sp.stdout.readlines():
				items = line.split(' ')
				if items[0] == 'rtt':
					rtt = items[3].split('/')[1]
					data['ping'] = float(rtt)
			sp.stdout.close()
			print data
			break
	if sp.poll() == None:
		sp.stdout.close()
		data['ping'] = -1
		print data
		sp.kill()
