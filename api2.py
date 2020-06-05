import flask, os
from flask import Flask, request, json
from flask_hashing import Hashing
import requests
from requests.exceptions import ConnectionError

app = Flask(__name__) # create flask object
hashing = Hashing(app)
app.config["DEBUG"] = True # set debugging option to true
key_value_store = {} # keeps track of all key value pairs
metadata_store = {} # keeps track of causal history
queue = [] # keeps track of requests that can not currently be delivered
nodehashes = {}

views = os.getenv('VIEW', default=None) #forwarding address = ip address of the main instance
views_list = []
if(views is not None):
    views_list = views.split(',')

socket_address = os.getenv('SOCKET_ADDRESS', default=None) # add this replica to the view list of each running replica
for v in views_list:
    ip = str(v).split(':')[0]
    h = hashing.hash_value(ip) #hash by ip address 
    nodehashes[v] = h #create list of the hashes of nodes to store somewhere
    if v != socket_address:
        try:
            requests.put('http://' + v + '/key-value-store-view', json={'socket-address': socket_address})
        except:
            continue

vector_clock = {}
for v in views_list: # initialize vector_clock only when this replica is first created, not when it is reconnected
    vector_clock[str(v)] = 0 # v is the socket address (key), 0 is the corresponding value

shardcount = int(os.getenv('SHARD_COUNT', default=None)) #this extracts the shard count set for the instances run
sharddict = {}
# creating shards with dictionary. 
for i in range(shardcount):
    sharddict[i+1] = []

j = 1
for i in range(len(views_list)):
    sharddict[j].append(views_list[i])
    j = j + 1
    if j > shardcount:
        j = 1

shardID = 1
for key in sharddict:
    if socket_address in sharddict[key]:
        shardID = key
@app.route('/store', methods=['GET']) # get key_value_store
def get_store():
    data = {"store": key_value_store}
    return data

@app.route('/store1', methods=['GET']) # get metadata_store
def get_store1():
    data = {"store": metadata_store}
    return data

@app.route('/broadcast/<key>', methods=['PUT', 'DELETE']) # write broadcast (replica to replica)
def broadcast(key):
    metadata = request.json.get('causal-metadata')
    val = request.json.get('value')
    if request.method == 'PUT' or request.method == 'DELETE': # regardless of the metadata being empty or not, increment the vector clock by one in the sender's position
        vector_clock[request.json.get('sender')] += 1 # increment local vector clock by one in the sender's position
        metadata_store[str(len(metadata_store))] = vector_clock.copy()
        if request.method == 'PUT':
            key_value_store[key] = val # updates local key value store
        if request.method == 'DELETE':
            key_value_store.pop(key, val)

@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def get_ids():
    if request.method == 'GET':
        data = {"message":"Shard IDs retrieved successfully","shard-ids": list(sharddict.keys())}
        status_code = 200
    return data, status_code

@app.route('/key-value-store-shard/shard-id-members/<id>', methods=['GET'])
def get_members(id):
    if request.method == 'GET':
        data = {"message":"Shard IDs retrieved successfully","shard-id-members": list(sharddict[int(id)])}
        status_code = 200
    return data, status_code

@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def get_shard_id():
    if request.method == 'GET':
        data = {"message":"Shard IDs retrieved successfully","shard-id": shardID}
        status_code = 200
    return data, status_code
# When a replica is reconnected, what happens to its vector clock? Current implementation is that it is updated as if it was sending and receiving messages
# Queue behavior?
@app.route('/key-value-store/<key>', methods=['PUT', 'GET', 'DELETE']) # main endpoint (client to replica)
def store_main(key):
    for v in views_list:
        if v != socket_address:
            try: # when a replica reconnects to the network, update its store by getting one from a running replica
                requests.put('http://' + str(v) + '/key-value-store-view', timeout=1, json={'socket-address': socket_address})
                r = requests.get('http://' + str(v) + '/store')
                key_value_store.update(r.json()['store']) # update local key value store upon reconnection
                m = requests.get('http://' + str(v) + '/store1')
                metadata_store.update(m.json()['store']) # update local metadata store upon reconnection
                vector_clock.update(metadata_store[str(len(metadata_store) - 1)]) # make sure the vector clock is the most recent one
            except: # replica is down, no need to do anything here
                continue

    if request.method == 'PUT' or request.method == 'DELETE': 
        metadata = request.json.get('causal-metadata') # causal metadata extraction from curl command
        vector_clock[socket_address] += 1 # increment local vector clock by one in its own position
        if key in key_value_store:
            status_code = 200 # update existing key's value
        else:
            if request.method == 'DELETE':
                data = {"message": "Key does not exit", "error": "Error in DELETE"}
                status_code = 404
                return data, status_code
            status_code = 201 # add new key value pair
        val = request.json.get('value')
        if request.method == 'PUT':
            key_value_store[key] = val # updates local key value store
            data = {"message": "Added successfully", "causal-metadata": vector_clock, "shard-id": shardID}
        if request.method == 'DELETE':
            key_value_store.pop(key, val)
            data = {"message": "Deleted successfully", "causal-metadata": vector_clock}
        metadata_store[str(len(metadata_store))] = vector_clock.copy()
        for replica in vector_clock.keys():
           if replica != socket_address:
               try: # broadcast request to every other running replica
                  if request.method == 'PUT':
                    requests.put('http://' + replica + '/broadcast/' + key, timeout=1, json={'sender': socket_address, 'value': val, 'causal-metadata': metadata})
                  if request.method == 'DELETE':
                    requests.delete('http://' + replica + '/broadcast/' + key, timeout=1, json={'sender': socket_address, 'value': val, 'causal-metadata': metadata})
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
        data = {"message": "View retrieved successfully", "view": vi, "store": key_value_store, "causal-metadata": metadata_store, "vector-clock": vector_clock, "nh":nodehashes}  # last two aren't necessary, but helpful info
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