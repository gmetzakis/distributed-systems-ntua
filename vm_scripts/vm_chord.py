from flask import Flask, render_template, request
import hashlib
import collections
import requests
from threading import Thread
import sys
import time
app = Flask(__name__)

keys = {}

bootstrap_ip = 'http://192.168.0.1:5000'
joined = False

def checkIfImResponsible(key):

    if int(pred_id)<int(node_id):
        return (key > int(pred_id) and key <= int(node_id))
    else:
        return (key > int(pred_id) or key <= int(node_id))

@app.route('/init',methods = ['POST','GET'])
def init():
    if request.method == 'POST':

        result = request.form.to_dict()

        global node_id
        global pred_id
        global suc_ip
        global replicas
        

        node_id = result["NodeID"]
        pred_id = result["Pred_id"]
        suc_ip = result["Suc_ip"]
        replicas = int(result["replicas"])

        if replicas:
            global replicas_method
            global k_rep
            replicas_method = str(result["replicas_method"])
            k_rep = int(result["k_rep"])

        print("nodeID: ", node_id)
        print("pred_id: ", pred_id)
        print("suc_ip: ", suc_ip, '\n')
        return render_template('messages.html', number=10)


@app.route('/update_suc',methods = ['POST','GET'])
def update_suc():
    if request.method == 'POST':
        result = request.form.to_dict()

        global suc_ip
        suc_ip = result["Suc_ip"]

        print("nodeID: ", node_id)
        print("pred_id: ", pred_id)
        print("suc_ip: ", suc_ip, '\n')
        return render_template('messages.html', number=10)


@app.route('/update_pred_join',methods = ['POST','GET'])
def update_pred_join():
    if request.method == 'POST':
        result = request.form.to_dict()

        global pred_id

        pred_id = result["Pred_id"]
        pred_ip = result["Pred_ip"]

        for k in keys.copy():
            if not checkIfImResponsible(int(k)):
                if not replicas:
                    requests.post(str(pred_ip+'/insert'), data = {'Key':keys[k][0], 'Value':keys[k][1]})
                    del keys[k]
                else:
                    if (int(keys[k][2]) < (k_rep-1)):
                        requests.post(str(suc_ip+'/delete_replicas'), data = {'k_rep':int(keys[k][2])+1,'Key':keys[k][0]})
                        requests.post(str(pred_ip+'/insert_replicas'), data = {'k_rep':int(keys[k][2]), 'Key':keys[k][0], 'Value':keys[k][1]})
                    else:
                        requests.post(str(pred_ip+'/insert_replicas'), data = {'k_rep':int(keys[k][2]), 'Key':keys[k][0], 'Value':keys[k][1]})
                        del keys[k]


        print("nodeID: ", node_id)
        print("pred_id: ", pred_id)
        print("suc_ip: ", suc_ip)

        return render_template('messages.html', number=10)


@app.route('/update_pred_depart',methods = ['POST','GET'])
def update_pred_depart():
    if request.method == 'POST':
        result = request.form.to_dict()

        global pred_id
        pred_id = result["Pred_id"]

        print("nodeID: ", node_id)
        print("pred_id: ", pred_id)
        print("suc_ip: ", suc_ip, '\n')

        return render_template('messages.html', number=10)


@app.route('/update_depart',methods = ['POST','GET'])
def update_depart():
    if request.method == 'POST':
        result = request.form.to_dict()

        for k in keys.copy():
            if not replicas:
                requests.post(str(suc_ip+'/insert'), data = {'Key':keys[k][0], 'Value':keys[k][1]})
                del keys[k]
            else:
                requests.post(str(suc_ip+'/insert_replicas'), data = {'k_rep':int(keys[k][2]),'Key':keys[k][0], 'Value':keys[k][1]})

        return render_template('messages.html', number=10)



@app.route('/insert',methods = ['POST','GET'])
def insert():
    if request.method == 'POST':
        result = request.form.to_dict()

        h = hashlib.sha1()
        h.update(result["Key"].encode())
        tmp = h.hexdigest()
        hashed_key = int(tmp, 16)

        if checkIfImResponsible(hashed_key):
            if replicas:
                rep = 0
                keys[hashed_key]=[result["Key"],result["Value"],rep]
                if replicas_method=="lin":
                    return requests.post(str(suc_ip+'/insert_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"], 'Value':result["Value"]}).content
                else:
                    '''
                    @app.after_response
                    def forwarding():
                        requests.post(str(suc_ip+'/insert_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"], 'Value':result["Value"]})
                    return render_template('messages.html', number=6, x = result["Key"], y=result["Value"], z=node_id)
                    
                    try:
                        return render_template('messages.html', number=6, x = result["Key"], y=result["Value"], z=node_id)
                    finally:
                        requests.post(str(suc_ip+'/insert_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"], 'Value':result["Value"]})
                    '''
                    def forwarding(successor_ip, k_replicas, key, value):
                        requests.post(str(successor_ip+'/insert_replicas'), data = {'k_rep':(k_replicas), 'Key':(key), 'Value':(value)})
                        return 1
                    thread = Thread(target=forwarding, kwargs={'successor_ip':suc_ip, 'k_replicas':(rep+1), 'key':result["Key"],'value':result["Value"]})
                    thread.start()
                    return render_template('messages.html', number=6, x = result["Key"], y=result["Value"], z=node_id)

            else:
                keys[hashed_key]=[result["Key"],result["Value"]]
                return render_template('messages.html', number=3, x = result["Key"], y=result["Value"], z=node_id)
        else:
            return requests.post(str(suc_ip+'/insert'), data = result).content


@app.route('/insert_replicas',methods = ['POST','GET'])
def insert_replicas():
    if request.method == 'POST':
        result = request.form.to_dict()
        rep = int(result["k_rep"])

        h = hashlib.sha1()
        h.update(result["Key"].encode())
        tmp = h.hexdigest()
        hashed_key = int(tmp, 16)

        keys[hashed_key]=[result["Key"],result["Value"],rep]

        if (rep==(k_rep-1)):    #done after this
            return render_template('messages.html', number=5, x = result["Key"], y=result["Value"], z=node_id)
        else:
            if replicas_method=="lin":
                #time.sleep(0.05)
                return requests.post(str(suc_ip+'/insert_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"], 'Value':result["Value"]}).content
            else:
                '''
                @app.after_response
                def rep_forwarding():
                    requests.post(str(suc_ip+'/insert_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"], 'Value':result["Value"]})
                return render_template('messages.html', number=6, x = result["Key"], y=result["Value"], z=node_id)
                
                try:
                    return render_template('messages.html', number=6, x = result["Key"], y=result["Value"], z=node_id)
                finally:
                    requests.post(str(suc_ip+'/insert_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"], 'Value':result["Value"]})
                '''
                def rep_forwarding(successor_ip, k_replicas, key, value):
                    requests.post(str(successor_ip+'/insert_replicas'), data = {'k_rep':(k_replicas), 'Key':(key), 'Value':(value)})
                    return 1
                thread = Thread(target=rep_forwarding, kwargs={'successor_ip':suc_ip, 'k_replicas':(rep+1), 'key':result["Key"],'value':result["Value"]})
                thread.start()
                return render_template('messages.html', number=6, x = result["Key"], y=result["Value"], z=node_id)



@app.route('/delete',methods = ['POST','GET'])
def delete():
    if request.method == 'POST':
        result = request.form.to_dict()

        h = hashlib.sha1()
        h.update(result["Key"].encode())
        tmp = h.hexdigest()
        hashed_key = int(tmp, 16)

        if checkIfImResponsible(hashed_key):
            if (any(key in keys.keys() for key in [hashed_key])):
                del keys[hashed_key]
                if replicas:
                    rep = 0
                    if replicas_method=="lin":
                        return requests.post(str(suc_ip+'/delete_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"]}).content
                    else:
                        '''
                        @app.after_response
                        def delete_forward():
                            requests.post(str(suc_ip+'/delete_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"]})
                        return render_template('messages.html', number=8, x = result["Key"], z=node_id)
                        
                        try:
                            return render_template('messages.html', number=8, x = result["Key"], z=node_id)
                        finally:
                            requests.post(str(suc_ip+'/delete_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"]})
                        '''
                        def delete_forward(successor_ip, k_replicas, key):
                            requests.post(str(successor_ip+'/delete_replicas'), data = {'k_rep':(k_replicas), 'Key':(key)})
                            return 1
                        thread = Thread(target=delete_forward, kwargs={'successor_ip':suc_ip, 'k_replicas':(rep+1), 'key':result["Key"]})
                        thread.start()
                        return render_template('messages.html', number=8, x = result["Key"], z=node_id)

                else:
                    return render_template('messages.html', number=4, x = result["Key"], y=node_id)
            else:
                return render_template('messages.html', number=11, x=result["Key"])
        else:
            return requests.post(str(suc_ip+'/delete'), data = result).content


@app.route('/delete_replicas',methods = ['POST','GET'])
def delete_replicas():
    if request.method == 'POST':
        result = request.form.to_dict()
        rep = int(result["k_rep"])

        h = hashlib.sha1()
        h.update(result["Key"].encode())
        tmp = h.hexdigest()
        hashed_key = int(tmp, 16)

        del keys[hashed_key]

        if (rep==(k_rep-1)):    #done after this
            return render_template('messages.html', number=7, x = result["Key"], z=node_id)
        else:
            if replicas_method=="lin":
                return requests.post(str(suc_ip+'/delete_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"]}).content
            else:
                '''
                @app.after_response
                def delete_replica_forward():
                    requests.post(str(suc_ip+'/delete_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"]})
                return render_template('messages.html', number=8, x = result["Key"], z=node_id)
                
                try:
                    return render_template('messages.html', number=8, x = result["Key"], z=node_id)
                finally:
                    requests.post(str(suc_ip+'/delete_replicas'), data = {'k_rep':(rep+1), 'Key':result["Key"]})
                '''
                def delete_replica_forward(successor_ip, k_replicas, key):
                    requests.post(str(successor_ip+'/delete_replicas'), data = {'k_rep':(k_replicas), 'Key':(key)})
                    return 1
                thread = Thread(target=delete_replica_forward, kwargs={'successor_ip':suc_ip, 'k_replicas':(rep+1), 'key':result["Key"]})
                thread.start()
                return render_template('messages.html', number=8, x = result["Key"], z=node_id)


@app.route('/query',methods = ['POST','GET'])
def query():
    if request.method == 'POST':
        result = request.form.to_dict()

        if result["Key"]=="*":
            tmp = {node_id: keys}
            return requests.post(str(suc_ip+'/query_star'), json = tmp).content

        else:
            h = hashlib.sha1()
            h.update(result["Key"].encode())
            tmp = h.hexdigest()
            hashed_key = int(tmp, 16)

            if (any(key in keys.keys() for key in [hashed_key])):
                if (replicas and replicas_method=="lin"):
                    if (keys[hashed_key][2] == (k_rep-1)):
                        return render_template('messages.html', number=9, x = result["Key"], y=node_id, z=keys[hashed_key][1])    #found
                    else:
                        return requests.post(str(suc_ip+'/query'), data = result).content
                else:
                    return render_template('messages.html', number=9, x = result["Key"], y=node_id, z=keys[hashed_key][1])    #found    #found
            else:
                if checkIfImResponsible(hashed_key):
                    return render_template('messages.html', number=11, x=result["Key"])
                else:
                    return requests.post(str(suc_ip+'/query'), data = result).content


@app.route('/query_star',methods = ['POST','GET'])
def query_star():
    if request.method == 'POST':
        result = request.get_json()
        if (any(('%s'%key) in result.keys() for key in [node_id])):  #done
            res = collections.OrderedDict(sorted(result.items()))
            return render_template('query_star.html', dict = res)
        else:
            tmp = {node_id: keys}
            result.update(tmp)
            return requests.post(str(suc_ip+'/query_star'), json = result).content


@app.route('/',methods = ['POST', 'GET'])
def home():
    if request.method == 'GET':
        global joined
        if not joined:
            joined = True
            requests.post(str(bootstrap_ip+'/join'), data = {'IPAddress':"http://"+node_ip, 'Port':node_port})
        return render_template('menu.html', ip=request.host)


@app.route('/overlay_client',methods = ['POST', 'GET'])
def overlay_client():
    if request.method == 'GET':
        return requests.get(str(bootstrap_ip+'/overlay')).content


@app.route('/depart_client',methods = ['POST', 'GET'])
def depart_client():
    if request.method == 'POST':
        try:
            return requests.post(str(bootstrap_ip+'/depart'), data = {'nodeID':node_id}).content
        finally:
            func = request.environ.get('werkzeug.server.shutdown')
            func()

@app.route('/help',methods = ['POST', 'GET'])
def get_help():
    if request.method == 'GET':
        return render_template('messages.html', number=13)

def start_runner():
    def start_loop():
        not_started = True
        while not_started:
            print('Starting..')
            try:
                r = requests.get('http://'+node_ip+':'+str(node_port))
                if r.status_code == 200:
                    not_started = False
            except:
                print("Not yet started")
            time.sleep(2)

    #print('Started runner')
    thread = Thread(target=start_loop)
    thread.start()


if __name__ == '__main__':
    node_ip0 = '192.168.0.'
    node_ip1 = str(sys.argv[1])
    node_ip = node_ip0+node_ip1

    node_port = int(sys.argv[2])
    
    start_runner()
    app.run(host=node_ip,port=node_port,debug = True)
