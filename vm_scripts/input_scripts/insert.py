from flask import Flask, render_template, request
import random
import requests
import time
#app = Flask(__name__)

if __name__ == '__main__':
	file1 = open('../input_files/insert.txt', 'r')
	Lines = file1.readlines()

	ips = ['http://192.168.0.1:5000', 'http://192.168.0.1:5001',
       'http://192.168.0.2:5000', 'http://192.168.0.2:5001',
       'http://192.168.0.3:5000', 'http://192.168.0.3:5001',
       'http://192.168.0.4:5000', 'http://192.168.0.4:5001',
       'http://192.168.0.5:5000', 'http://192.168.0.5:5001']

	start_time = time.time()
	for line in Lines:
	    full = (line.strip().split(", "))
	    key = full[0]
        value = full[1]
        ip = random.choice(ips)
	    requests.post(str(ip+'/insert'), data = {'Key': key, 'Value': value})
	end_time = time.time()
	total_time = end_time - start_time
	print("TOTAL TIME: ", total_time)

