import flask, os
from flask import Flask, request, json
import requests
from requests.exceptions import ConnectionError

app = Flask(__name__) # create flask object
app.config["DEBUG"] = True # set debugging option to true
key_value_store = {} # keeps track of all key value pairs
metadata_store = [] # keeps track of causal history
disconnected_replicas = []

views = os.getenv('VIEW', default=None) #forwarding address = ip address of the main instance
views_list = []
if(views is not None):
    views_list = views.split(',')

socket_address = os.getenv('SOCKET_ADDRESS', default=None) # add this replica to the view list of each running replica
for v in views_list:
    if v != socket_address:
        try:
            requests.put('http://' + v + '/key-value-store-view', json={'socket-address': socket_address})
        except:
            continue

vector_clock = {}
for v in views_list: # initialize vector_clock only when this replica is first created, not when it is reconnected
    vector_clock[str(v)] = 0 # v is the socket address (key), 0 is the corresponding value

@app.route('/store', methods=['GET'])
def get_store():
    data = {"store": key_value_store}
    return data

@app.route('/broadcast/<key>', methods=['PUT'])
def broadcast(key):
    metadata = request.json.get('causal-metadata')
    #IMPLEMENT CAUSAL CONSISTENCY HERE
    if request.method == 'PUT': # regardless of the metadata being empty or not, increment the vector clock by one in the sender's position
        vector_clock[request.json.get('sender')] += 1 # increment local vector clock by one in the sender's position
        key_value_store[key] = request.json.get('value') # updates local key value store
        metadata_store.append(vector_clock) # updates local causal metadata store

@app.route('/key-value-store/<key>', methods=['PUT', 'GET', 'DELETE'])
def store_main(key):
    #IMPLEMENT CAUSAL CONSISTENCY HERE
    for v in views_list:
        if v != socket_address:
            try: # when a replica reconnects to the network, update its store by getting one from a running replica
                requests.put('http://' + str(v) + '/key-value-store-view', timeout=1, json={'socket-address': socket_address})
                r = requests.get('http://' + str(v) + '/store')
                key_value_store.update(r.json()['store']) # update local key value store
            except: # replica is down, no need to do anything here
                continue
    if request.method == 'PUT':
        metadata = request.json.get('causal-metadata') # causal metadata extraction from curl command
        vector_clock[socket_address] += 1 # increment local vector clock by one in its own position
        if key in key_value_store:
            status_code = 200 # update existing key's value
        else:
            status_code = 201 # add new key value pair
        key_value_store[key] = request.json.get('value') # updates local key value store
        metadata_store.append(vector_clock) # updates local causal metadata store
        data = {"message": "Added successfully", "causal-metadata": vector_clock}
        for replica in vector_clock.keys():
           if replica != socket_address:
               try: # broadcast request to every other running replica
                  requests.put('http://' + replica + '/broadcast/' + key, timeout=1, json={'sender': socket_address, 'value': request.json.get('value'), 'causal-metadata': metadata})
               except ConnectionError: # replica is either disconnected or killed
                    for v in views_list:
                        if replica != v:
                            try: # regardless if the replica is disconnected or killed, remove it from every running replica's view list
                                requests.delete('http://' + v + '/key-value-store-view', timeout=1, json={'socket-address': replica})
                            except:
                                continue
        return data, status_code
    if request.method == 'GET':
        if key in key_value_store.keys():
            data = {"message": "Value retrieved successfully", "value": key_value_store[key], "causal-metadata": vector_clock}
            status_code = 200
        else:
            data = {"message": "Key does not exist","error": "Error in GET"}
            status_code = 404
        return data, status_code

@app.route('/key-value-store-view', methods=['PUT', 'GET', 'DELETE'])
def view_main():
    if request.method == 'PUT':
        sa = request.json.get('socket-address')
        if sa not in views_list:
            views_list.append(sa) # add the passed in socket address to this replica's views list
            data = {"message": "Replica added successfully to the view"}
            status_code = 201
            return data, status_code

    if request.method == 'GET':
        vi = ''
        for idx, v in enumerate(views_list):
            vi += str(v)
            if idx != len(views_list) - 1:
                vi += ',' # test script wants a specific format
        data = {"message": "View retrieved successfully", "view": vi, "store": key_value_store, "causal-metadata": metadata_store}  # last two aren't necessary, but helpful info
        status_code = 200
        return data, status_code

    if request.method == 'DELETE':
        sa = request.json.get('socket-address')
        if sa in views_list:
            views_list.remove(sa)
            data = {"message": "Replica deleted successfully from the view"}
            status_code = 200
        else:
            data =  {"error": "Socket address does not exist in the view", "message": "Error in DELETE"}
            status_code = 404
        return data, status_code

app.run(host="0.0.0.0", port=8085) 