from flask import Flask, render_template, request
import hashlib
import random
import requests
import time
import sys
from datetime import datetime
app = Flask(__name__)

# Using readlines()
if __name__ == '__main__':
    rep_type = str(sys.argv[1])
    insert_lst, query_lst = [], []

    file1 = open('../input_files/requests.txt', 'r')
    Lines = file1.readlines()

    ips = ['http://192.168.0.1:5000', 'http://192.168.0.1:5001',
           'http://192.168.0.2:5000', 'http://192.168.0.2:5001',
           'http://192.168.0.3:5000', 'http://192.168.0.3:5001',
           'http://192.168.0.4:5000', 'http://192.168.0.4:5001',
           'http://192.168.0.5:5000', 'http://192.168.0.5:5001']

    start_time = time.time()
    for line in Lines:
        full = (line.strip().split(", "))
        if full[0] == 'insert':
            key = full[1]
            value = full[2]
            ip = random.choice(ips)
            requests.post(str(ip+'/insert'), data = {'Key': key, 'Value': value})
        elif full[0] == 'query':
            key = full[1]
            ip = random.choice(ips)
            query_lst.append((requests.post(str(ip+'/query'), data = {'Key': key}).content).decode("utf-8") )

    end_time = time.time()
    total_time = end_time - start_time
    print("----------------------------")
    print("TOTAL TIME: ", total_time)
    print()
    print("----------------------------")
    print()
    print("QUERY RESPONSE LIST:")
    print(query_lst)
    print()
    with open('test_out_'+rep_type+'.txt', 'w') as f:
        for item in query_lst:
            f.write("%s\n" % item)
