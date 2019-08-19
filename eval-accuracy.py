import os
import subprocess
import sys

path = sys.argv[1]

for obj in ['counts', 'new', 'open']:
	for method in ['const', 'ttl', 'pattern1', 'pattern2', 'pattern3']:
		subprocess.call(['go', 'run', 'eval-accuracy.go', '{}/final_{}_{}.json'.format(path, method, obj), 'gt_{}.json'.format(obj)])
