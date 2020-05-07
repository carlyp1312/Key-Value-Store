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
envvar = os.getenv('FORWARDING_ADDRESS', default=None) #forwarding address = ip address of the main instance
if(envvar is not None):
    baseUrl = 'http://' + str(envvar)
    isMain = False

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