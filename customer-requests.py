import requests
import json, jsonpickle
import os
import sys
import base64

REST = os.getenv("REST") or "127.0.0.1:5000"

def mkReq(reqmethod, endpoint, data):
    print(f"Response to http://{REST}/{endpoint} request is {type(data)}")
    response = reqmethod(f"http://{REST}/{endpoint}", json=data)
                        #  headers={'Content-type': 'application/json'})
    if response.status_code == 200:
        jsonResponse = json.dumps(response.json(), indent=4, sort_keys=True)
        print(jsonResponse)
        return
    else:
        print(
            f"response code is {response.status_code}, raw response is {response.text}")
        return response.text


print("Add Customer")
mkReq(requests.post, "customer", 
    data={
            "name": "Global Solutions",
            "mobile": "9876543210",
            "email": "globalsolutions@gmail.com"
        }
    )

sys.exit(0)