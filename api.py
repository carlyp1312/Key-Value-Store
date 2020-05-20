adatimport flask, os
from flask import Flask, request, json
# import docker
import requests
from requests.exceptions import ConnectionError 
#create flask object and set debugging option to true
app = Flask(__name__)
app.config["DEBUG"] = True
key_value_store = {} # keeps track of all key value pairs
metadata_store = {} # keeps track of all causal history of this replica

# this file represents just one replica
# we can get this replica's vector clock position through its socket address position in the view environmental variable
# from there we can start comparing vector clocks
# replica's causal metadata = vector_clock, sender's causal metadata = vc
# print(os.environ['FORWARDING_ADDRESS'])

isMain = True
views = os.getenv('VIEW', default=None) #forwarding address = ip address of the main instance
if(views is not None):
    views_list = views.split(',')
    
socket_address = os.getenv('SOCKET_ADDRESS', default=None)
if(socket_address is not None):
    baseUrl = 'http://' + str(socket_address)

vector_clock = {}
buffer = []
sendpos = 0

#initialize, send, recieve, increment@, max functions

def initialize():
    for v in views_list:
        vector_clock[v] = 0 # v is the socket address (key), 0 is the corresponding value

def pointwiseMax(vc):
    new_vector_clock = []
    for i in range(len(vector_clock)):
        new_vector_clock.append(max(vector_clock[i], vc[i]))
    return new_vector_clock 


def isLessThan(vc):
    if len(vc) == 0:
        return False

    for key in vc:
        if(vc[key] > vector_clock[key]):
            addToQueue(vc)
            return False
    return True


# #assumption: incremented before it was sent REMEMBER TO DO THAT 
# #might need to add another parameter
# def canDeliver(vc, sp): 
#     for i in vc:
#         #
#         if(i != sp):
#             if(vc[i] > vector_clock[i]):
#                 addtoQueue(vc)
#                 return False
#                 break
#         # only client sends requests, but client doesnt have a position 
#         # in the vector clock, so this comparison below probably 
#         # doesn't work. we have to check for client as some 3rd party source,
#         # to buffer requests and get everything causally ordered.  
#         elif(i==sp):
#             if(vc[i] + 1 != vector_clock[i]):
#                 addtoQueue(vc)
#                 return False 
#                 break
#         else:
# #             # and this is client code
                 
#     return True

def addToQueue(vc):
    buffer.append(vc)
    time.sleep(1)

def removeFromQueue(vc):
    buffer.remove(0)
    
 

# This endpoint below will specify which replica getting the curl request, 
# when the @app.route functionality gets the socket address of the replica
# from the curl request endpoint.
@app.route('/key-value-store/<key>', methods=['PUT', 'GET', 'DELETE'])
def main(key):
    #is this the vector clock were looking for 
    #PUT request

    metadata = request.json.get('causal-metadata') # causal metadata extraction from curl command
    if metadata == '':
        initialize() # set all vector clock positions to zero
        if request.method == 'PUT':
            # first generates causal metadata
            for i in vector_clock:
                if i == socket_address:
                    vector_clock[i] += 1
                    sendpos = i
            key_value_store[key] = request.json.get('value') #updates local key value store
            metadata_store.append(vector_clock) #updates LOCAL causal metadata store 
            data = '{"message":"Added successfully", "causal-metadata": ' + vector_clock + '}'
            status_code = 201
            # tores , broadcoast to obroadcastcas
            for i in vector_clock:
                if i != socket_address: # broadcast to every replica other than current replica
                    r = requests.put(baseUrl + '/key-value-store/' + str(key), json={'value': request.json.get('value')})          
            return data, status_code
        else:
            if request.method == 'GET':
                data = {"error":"Non-existent key","message":"Error in GET"}
                status_code = 404
            if request.method == 'DELETE':
                data = {"error":"Non-existent key","message":"Error in DELETE"}
                status_code = 404
                
    #WHEN METADATA IS NOT EMPTY
    else:
        actualsender = sendpos
        #check if same replica
        for i in vector_clock:
            if i == socket_address:
                vector_clock[i] += 1
                sendpos = i


        if isLessThan(metadata) == True:
            #execute request 
        
        if isLessThan(buffer[0]) == True:
            removeFromQueue(buffer[0])
        

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