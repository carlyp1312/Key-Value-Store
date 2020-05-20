import flask, os
import ast
from flask import Flask, request, json
# import docker
import requests
from requests.exceptions import ConnectionError 
#create flask object and set debugging option to true
app = Flask(__name__)
app.config["DEBUG"] = True
key_value_store = {} # keeps track of all key value pairs
metadata_store = [] # keeps track of all causal history of this replica

views = os.getenv('VIEW', default=None) #forwarding address = ip address of the main instance
views_list = []
if(views is not None):
    views_list = views.split(',')

vector_clock = {}
for v in views_list:
    vector_clock[str(v)] = 0 # v is the socket address (key), 0 is the corresponding value
socket_address = os.getenv('SOCKET_ADDRESS', default=None)

for v in views_list:
    if v != socket_address:
        try:
            r = requests.put('http://' + str(v) + '/key-value-store-view', timeout = 1, json={'socket-address': str(socket_address)})
        except:
            continue
        try:
            r = requests.get('http://' + str(v) + '/key-value-store-view', timeout = 1)
            key_value_store = ast.literal_eval(r.json()['store'])
        except:
            continue
buffer = []

def isLessThan(vc):
    if vc == "":
        return False
    for key in vc:
        if(vc[key] > vector_clock[key]):
            addToQueue(vc)
            return False
    return True

def addToQueue(vc):
    buffer.append(vc)
    time.sleep(1)

def removeFromQueue():
    buffer.remove(0)

#AND DELETE
@app.route('/key-value-store2/<key>', methods=['PUT'])
def main2(key):
    metadata = request.json.get('causal-metadata')
    if isLessThan(metadata) == True:
        if request.method == 'PUT': # regardless of the metadata being empty or not, increment the vector clock by one in the sender's position
            # first generates causal metadata
            #IMPLEMENT CAUSAL-CONSISTENCY HERE
            vector_clock[request.json.get('sender')] += 1
            key_value_store[key] = request.json.get('value') #updates local key value store
            metadata_store.append(vector_clock) #updates local causal metadata store

    if isLessThan(buffer[0]) == True:
        if request.method == 'PUT': # regardless of the metadata being empty or not, increment the vector clock by one in the sender's position
            # first generates causal metadata
            #IMPLEMENT CAUSAL-CONSISTENCY HERE
            vector_clock[request.json.get('sender')] += 1
            key_value_store[key] = request.json.get('value') #updates local key value store
            metadata_store.append(vector_clock) #updates local causal metadata store
        removeFromQueue()

@app.route('/key-value-store/<key>', methods=['PUT', 'GET', 'DELETE'])
def main(key):
    data = {}
    status_code = 0
    #IMPLEMENT CAUSAL-CONSISTENCY HERE
    metadata = request.json.get('causal-metadata') # causal metadata extraction from curl command
    if metadata == '' or isLessThan(metadata) == True:
        if request.method == 'PUT':
            # first generates causal metadata
            vector_clock[socket_address] += 1
            if key in key_value_store.keys():
                status_code = 200 # update existing key
            else:
                status_code = 201 # adding new key

            key_value_store[key] = request.json.get('value') #updates local key value store
            metadata_store.append(vector_clock) #updates local causal metadata store
            data = '{"message": "Added successfully","store": "' + str(key_value_store) + '","causal-metadata": "' + str(vector_clock) + '"}'
        
            for i in vector_clock.keys():
                if i != socket_address: # broadcast to every replica other than current replicas
                    #check if replica is down
                    try:
                        r = requests.put('http://' + str(i) + '/key-value-store2/' + key, timeout=1, json={'sender': str(socket_address), 'value': request.json.get('value'), 'causal-metadata': metadata})
                    except:
                        # delete view
                        for v in views_list:
                            if i != v:
                                try:  
                                    r = requests.delete('http://' + str(v) + '/key-value-store-view', timeout=1, json={'socket-address': str(i)})
                                except:
                                    continue
                return data, status_code

        if request.method == 'GET':
            if key in key_value_store.keys():
                data = {"message":"Value retrieved successfully","value": key_value_store[key], "causal-metadata": str(vector_clock)}
                status_code = 200
            else:
                data = {"message":"Key does not exist","error":"Error in GET"}
                status_code = 200
            return data, status_code

    if isLessThan(buffer[0]) == True:
        if request.method == 'PUT':
            metadata = request.json.get('causal-metadata') # causal metadata extraction from curl command
            # first generates causal metadata
            vector_clock[socket_address] += 1
            if key in key_value_store.keys():
                status_code = 200 # update existing key
            else:
                status_code = 201 # adding new key
            key_value_store[key] = request.json.get('value') #updates local key value store
            metadata_store.append(vector_clock) #updates local causal metadata store
            data = '{"message": "Added successfully","store": "' + str(key_value_store) + '","causal-metadata": "' + str(vector_clock) + '"}'
            for i in vector_clock.keys():
                if i != socket_address: # broadcast to every replica other than current replicas
                    #check if replica is down
                    try:
                        r = requests.put('http://' + str(i) + '/key-value-store2/' + key, timeout=1, json={'sender': str(socket_address), 'value': request.json.get('value'), 'causal-metadata': metadata})
                    except:
                        # delete view
                        for v in views_list:
                            if i != v:
                                try:  
                                    r = requests.delete('http://' + str(v) + '/key-value-store-view', timeout=1, json={'socket-address': str(i)})
                                except:
                                    continue
            return data, status_code
        if request.method == 'GET':
            if key in key_value_store.keys():
                data = {"message":"Value retrieved successfully","value": key_value_store[key], "causal-metadata": str(vector_clock)}
                status_code = 200
            else:
                data = {"message":"Key does not exist","error":"Error in GET"}
                status_code = 200
            return data, status_code
        removeFromQueue(buffer[0])

@app.route('/key-value-store-view', methods=['PUT', 'GET', 'DELETE'])
def viewmain():
    if request.method == 'PUT':
        sa = request.json.get('socket-address')
        if sa not in views_list:
            views_list.append(sa)
            data = {"message":"Replica added successfully to the view"}
            status_code = 201
            return data, status_code
                
    if request.method == 'GET':
        vi = ''
        for idx, v in enumerate(views_list):
            vi += str(v)
            if idx != len(views_list) - 1:
                vi += ','
        #ADD METADATA STORE
        data = {"message":"View retrieved successfully","view":vi, "store":str(key_value_store)}
        status_code = 200
        return data, status_code


    if request.method == 'DELETE':
        sa = request.json.get('socket-address')
        if sa in views_list:
            views_list.remove(sa)
            data = {"message":"Replica deleted successfully from the view"}
            status_code = 200
        else:
            data =  {"error":"Socket address does not exist in the view", "message":"Error in DELETE"}
            status_code = 404
        return data, status_code
        
app.run(host="0.0.0.0", port=8085)