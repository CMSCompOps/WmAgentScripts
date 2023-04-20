
import json
import http.client
import os
import traceback
import urllib

class MSPileupClient():


    def __init__(self, url):

        self.CERT_FILE = os.getenv('X509_USER_PROXY')
        self.KEY_FILE = os.getenv('X509_USER_PROXY')
        self.url = url

    def getByPileupName(self, pileupName):
        """
        Queries ReqMgr through a HTTP GET method
        in every request manager query
        url: the instance used, i.e. url='cmsweb.cern.ch'
        request: the request suffix url
        retries: number of retries
        """
        endpoint = "/ms-pileup/data/pileup?pileupName="
        headers = {"Accept": "application/json"}

        try:
            conn = http.client.HTTPSConnection(self.url, cert_file=self.CERT_FILE, key_file=self.KEY_FILE)
        except Exception as e:
            print ("Exception while establishing https connection")
            print (str(e))
            return None

        try:

            r1 = conn.request("GET", endpoint + pileupName, headers=headers)
            r2 = conn.getresponse()
            print ("HTTP STATUS:", str(r2.status))
            response = json.loads(r2.read()) if r2.status == 200 else None
        except Exception as e:
            print ("Exception while getting response from MSPileup")
            print (str(e))
            return None

        return response

    def createPileupDocument(self, params):

        endpoint = "/ms-pileup/data/pileup"
        headers = {"Content-type": "application/json", "Accept": "application/json"}
        try:
            data = self.httpRequest("POST", self.url, endpoint, params, headers, encode=json.dumps)
            return data
        except Exception as e:
            print ("Pileup document creation failed")
            return None

    def updatePileupDocument(self, params):

        endpoint = "/ms-pileup/data/pileup"
        headers = {"Content-type": "application/json", "Accept": "application/json"}
        try:
            data = self.httpRequest("PUT", self.url, endpoint, params, headers, encode=json.dumps)
            return data
        except Exception as e:
            print ("Pileup document update failed")

    def httpRequest(self, verb, url, endpoint, params, headers, encode=urllib.parse.urlencode):

        cert_file = os.environ['HOME'] + '/.globus/usercert.pem'
        key_file = os.environ['HOME'] + '/.globus/userkey.pem'
        try:
            conn = http.client.HTTPSConnection(url, cert_file=cert_file, key_file=key_file)
            encodedParams = encode(params) if encode else params
            conn.request(verb, endpoint, encodedParams, headers)
            response = conn.getresponse()
            print ("HTTP STATUS:", str(conn.status))
            data = response.read() if conn.status == 200 else None
            conn.close()
            return data
        except Exception as e:
            print ("Exception while PUT request")
            print (str(e))
            print(traceback.format_exc())
            return None









