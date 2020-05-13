import flask, os
from flask import Flask, request, json
# import docker
import requests
from requests.exceptions import ConnectionError 
#create flask object and set debugging option to true
app = Flask(__name__)
app.config["DEBUG"] = True
store = {}

# print(os.environ['FORWARDING_ADDRESS'])
isMain = True
views = os.getenv('VIEW', default=None) #forwarding address = ip address of the main instance
if(views is not None):
    views_list = views.split(',')
    
socket_address = os.getenv('SOCKET_ADDRESS', default=None)
if(socket_address is not None):
    baseUrl = 'http://' + str(socket_address)
    isMain = False

vector_clock = []

#initialize, send, recieve, increment@, max functions

def initialize():
    for v in views_list:
        vector_clock.append(0)

def pointwiseMax(vc):
    new_vector_clock = []
    for i in range(len(vector_clock)):
        new_vector_clock.append(max(vector_clock[i], vc[i]))
    return new_vector_clock  

def isLessThan(vc):
    bools = []
    bools2 = []

    for i in range(len(vector_clock)):
        if(vector_clock[i] <= vc[i]):
            bools[i] = True
        else:
            bools[i] = False
    
    for i in range(len(vector_clock)):
        if(vc[i] <= vector_clock[i]):
            bools2[i] = True
        else:
            bools2[i] = False

    if False in bools & False in bools2:
        islessthan = "independent"

    allTrue = True
    for b in bools2:
        if b == False:
            allTrue = False 
            break
    
    if allTrue == True:
        islessthan = "causally (before)dependent"
        

# can bools have all trues? that means the receiving
# vector clock, vector_clock, happens before the passed
# in vector clock, vc. is this possible?
# it makes sense bools2 can have all trues, since the 
# passed-in vector clock, vc, happens before the receiving
# vector clock, vector_clock, which seems consistent with
# a diagram of a process sending a request and its vector clock,
# vc, to another process with its own vector clock, vector_clock.
# i.e. a diagram like this seems consistent with vc happening
# before vector_clock, not vector_clock happening before vc.

# A [2,2,4]
# B [1,2,3]
# A < B [F, T, F]
# B < A [T, T, T] 

@app.route('/key-value-store/<key>', methods=['PUT', 'GET', 'DELETE'])
def main(key):
    if isMain == False:
        #PUT request
        if request.method == 'PUT':
            try:
                r = requests.put(baseUrl + '/key-value-store/' + str(key), json={'value': request.json.get('value')})
                return r.content, r.status_code 
            except ConnectionError:
                data = '{"error":"Main instance is down","message":"Error in PUT"}'
                status_code = 503
                return data, status_code
        
        #GET request        
        if request.method == 'GET':
            try:
                r = requests.get(baseUrl + '/key-value-store/' + str(key))
                return r.content, r.status_code
            except ConnectionError:
                data = '{"error":"Main instance is down","message":"Error in GET"}'
                status_code = 503
                return data, status_code

        #DELETE request
        if request.method == 'DELETE':
            #requests.delete(envvar)     
            try:
                r = requests.delete(baseUrl + '/key-value-store/' + str(key))
                return r.content, r.status_code
            except ConnectionError:
                data = '{"error":"Main instance is down","message":"Error in DELETE"}'
                status_code = 503
                return data, status_code
    else:
        if request.method == 'PUT':
            value = request.json.get('value')
            if value != None:
                if key in store:
                    #update 
                    store[key] = value
                    data = '{"message":"Updated successfully","replaced":true}'
                    status_code = 200
                else:
                    if len(key) > 50:
                        #key too long
                        data ='{"error":"Key is too long","message":"Error in PUT"}'
                        status_code = 400
                    else:
                        #add 
                        store[key] = value  
                        data = '{"message":"Added successfully","replaced":false}'
                        status_code = 201
            else: 
                #value missing
                data = '{"error":"Value is missing","message":"Error in PUT"}'
                status_code = 400
        
        #GET request        
        if request.method == 'GET':
            if key in store:
                data ='{"doesExist":true,"message": "Retrieved successfully","value": "' + str(store[key]) + '"}'
                status_code = 200
            else:
                data = '{"doesExist":false,"error":"Key does not exist","message":"Error in GET"}'
                status_code = 404

        #DELETE request
        if request.method == 'DELETE':
            if key in store:
                del store[key]
                data = '{"doesExist":true,"message":"Deleted successfully"}'
                status_code = 200
            else:
                data = '{"doesExist":false,"error":"Key does not exist","message":"Error in DELETE"}'
                status_code = 404

        return data, status_code


@app.route('/key-value-store-view', methods=['PUT', 'GET', 'DELETE'])
def viewmain():
    if request.method == 'PUT':
        if socket_address in views_list:
            data = {"error":"Socket address already exists in the view","message":"Error in PUT"}
            status_code = 404
        else:
            #BROADCAST
            for v in views_list:
                print("V = ", v)
                print("LEN  = ", len(views_list))
                if(v != socket_address):
                    #CHECK IF WE NEED TO SEND VALS 
                    views_list.append(socket_address)
                    r = requests.put('http://' + str(v)+ '/key-value-store-view',  json={"socket-address": str(v)})
                    
            data = {"message":"Replica added successfully to the view"}
            status_code = 201

        return data, status_code
                
    if request.method == 'GET':
        if socket_address in views_list:
            data = {"message":"View retrieved successfully","view":views_list}
            status_code = 200
        else:
            #ignore in some sense, CHECK TEST SCRIPT
            data = {"error":"Socket address does not exist in the view", "message":"Error in GET"}
            status_code = 404
        return data, status_code


    if request.method == 'DELETE':
        if socket_address in views_list:
            views_list.remove(socket_address)
            #BROADCAST
            for v in views_list:
                if(v != socket_address):
                    r = requests.delete('http://' + str(v) + '/key-value-store-view')
            data = {"message":"Replica deleted successfully from the view"}
            status_code = 200
        else:
            data =  {"error":"Socket address does not exist in the view", "message":"Error in DELETE"}
            status_code = 404
        return data, status_code

    #Replica is actually down (TIME OUTS)

            
app.run(host="0.0.0.0", port=8085)


#extract from VIEW (cmd) using os.environ (-e) 
#implement corresponding operations (GET, []) (PUT, add) (DELETE, remove) 
#timeouts to detect when replicas are down. ex.-1 second (time library)
#   try catch, send requests
#broadcasting to other replicas (PUT, DELETE)
#   boolean flag + forward requests using requests library, some sort of looping functionality to cover all replicas
#   add replica: 
#      add to everyones view (list ops)
#      add key value store values
#   delete replica: 
#      remove from everyones view (list ops)


# vector clock implementation: list of length, # of replicas 