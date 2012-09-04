#!/usr/bin/python

import time
import subprocess
import sys
CWD = '/root/Monitor/src/'
sys.path.append(CWD)

IPMITOOL = '/usr/bin/ipmitool'

for node in open(CWD + 'nodes'):
	parts = node.rstrip('\r\n').split(' ')
	if len(parts) >= 4:
		t0 = time.time()
		timeout = 2
		[ip, hostname, ipmi_ip, ipmi_user, ipmi_pass] = parts
		args = [IPMITOOL, '-H', ipmi_ip, '-I', 'lan', '-U', ipmi_user, '-P', ipmi_pass, 'sdr']
		sp = subprocess.Popen(args, stdout=subprocess.PIPE)
		data = {}
		data['agent'] = ip
		data['hostname'] = hostname
		data['ipmi_ip'] = ipmi_ip
		while time.time() < t0 + timeout:
			if sp.poll() != None:
				for line in sp.stdout.readlines():
					items = line.rstrip('\r\n').split('|')
					metric_name = 'ipmi_'+'_'.join(items[0].split()).replace('.', 'd')
					metric_value = items[1].split()
					if not metric_value[1] in ['unspecified', 'reading']:
						data[metric_name] = float(metric_value[0])
				sp.stdout.close()
				print data
				break
		if sp.poll() == None:
			sp.stdout.close()
			print data
                	sp.kill()
