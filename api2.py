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
initialized = False

views = os.getenv('VIEW', default=None) #forwarding address = ip address of the main instance
views_list = []
if(views is not None):
    views_list = views.split(',')

socket_address = os.getenv('SOCKET_ADDRESS', default=None)
for v in views_list:
    if v != socket_address:
        try: # when this replica goes back up, each replica needs to add this replica to its view_list and vector_clock
            r = requests.put('http://' + str(v) + '/key-value-store-view', timeout = 1, json={'socket-address': str(socket_address)})
        except:
            continue
        try: # when a replica goes back up, it needs to get the key_value_store from one of the replicas
            r = requests.get('http://' + str(v) + '/key-value-store-view', timeout = 1) 
            key_value_store = ast.literal_eval(r.json()['store']) # this replica adds the key_value_store from replica v
        except:
            continue
vector_clock = {}
for v in views_list: # initialize the vector_clock only once - can do multiple operations without any causal metadata
    vector_clock[str(v)] = 0 # v is the socket address (key), 0 is the corresponding value
buffer = []

@app.route('/key-value-store2/<key>', methods=['PUT'])
def main2(key):
    metadata = request.json.get('causal-metadata')
    if request.method == 'PUT':
        vector_clock[request.json.get('sender')] += 1 # increment the vector_clock in the position of the sender by one
        # IMPLEMENT CAUSAL-CONSISTENCY HERE
        key_value_store[key] = request.json.get('value') #updates local key value store
        metadata_store.append(vector_clock) # updates local causal metadata store

@app.route('/key-value-store/<key>', methods=['PUT', 'GET', 'DELETE'])
def main(key):
    data = {}
    status_code = 0
    if request.method == 'PUT':
        metadata = request.json.get('causal-metadata') # causal metadata extraction from curl command
        vector_clock[socket_address] += 1 # increment the vector_clock in the position of the sender by one
        # IMPLEMENT CAUSAL-CONSISTENCY HERE
        if key in key_value_store.keys():
            status_code = 200 # update existing key
        else:
            status_code = 201 # adding new key
        key_value_store[key] = request.json.get('value') # updates local key value store
        metadata_store.append(vector_clock) # updates local causal metadata store
        data = '{"message": "Added successfully","vector-clock": "' + str(vector_clock) + '","store": "' + str(key_value_store) + '","causal-metadata": "' + str(vector_clock) + '"}'
        vector_clock_copy = vector_clock.copy() # since we are possibly changing vector_clock within the try/except, make a copy.
        for i in vector_clock_copy.keys():
           if i != socket_address: # broadcast to every replica other than current replicas
               try: # check if replica is down. if it is not, add this put operation to the other replicas (broadcast put)
                  r = requests.put('http://' + str(i) + '/key-value-store2/' + key, timeout=1, json={'sender': str(socket_address), 'value': request.json.get('value'), 'causal-metadata': metadata})
               except: # if replica is down, delete this replica from every replica's view_list and vector_clock (broadcast delete)
                  views_list_copy = views_list.copy() # since we are possibly changing views_list within the try/except, make a copy.
                  for v in views_list_copy:
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
            status_code = 404
        return data, status_code

@app.route('/key-value-store-view', methods=['PUT', 'GET', 'DELETE'])
def viewmain():
    if request.method == 'PUT':
        sa = request.json.get('socket-address')
        if sa not in vector_clock.keys(): # add this socket address with a value of 0 to vector_clock
            vector_clock[sa] = 0
        if sa not in views_list: # add this socket address to the views_list
            views_list.append(sa)
            data = {"message":"Replica added successfully to the view"}
            status_code = 201
            return data, status_code
                
    if request.method == 'GET':
        vi = ''
        for idx, v in enumerate(views_list):
            vi += str(v)
            if idx != len(views_list) - 1:
                vi += ',' # views_list needs to be in this specific format
        data = {"message":"View retrieved successfully","view": vi, "store": str(key_value_store), "vector-clock": str(vector_clock), "causal-metadata": str(metadata_store)}
        status_code = 200
        return data, status_code


    if request.method == 'DELETE':
        sa = request.json.get('socket-address')
        if sa in views_list: 
            views_list.remove(sa) # remove this socket address from views_list
            del vector_clock[sa] # remove this socket address from vector_clock
            data = {"message":"Replica deleted successfully from the view"}
            status_code = 200
        else:
            data =  {"error":"Socket address does not exist in the view", "message":"Error in DELETE"}
            status_code = 404
        return data, status_code
        
app.run(host="0.0.0.0", port=8085)