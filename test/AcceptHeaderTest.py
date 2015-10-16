#!/usr/bin/env python 
import httplib
import os

def httpsget(host,
             url,
             headers={},
             port=443,
             certpath="/data/certs/servicecert.pem",
             keypath="/data/certs/servicekey.pem",
             password=None):
    conn = httplib.HTTPSConnection(host=host,
                                   port=port,
                                   key_file=keypath,
                                   cert_file=certpath)
    conn.connect()
    conn.request("GET", url=url, headers=headers)
    response = conn.getresponse()
    status = response.status
    content = response.read()
    conn.close()
    return status, content

url = "cmsweb.cern.ch"
conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_CERT'), key_file = os.getenv('X509_USER_KEY'))
#r1=conn.request('GET','/reqmgr2/data/request?name='+wf[0],headers={"Accept": "application/json"})
r1=conn.request('GET','/reqmgr2/data/request?name=heli_RVCMSSW_7_5_3ZTT_13_PUpmx25ns__FastSim_150925_132658_5738', 
                headers={"Accept":"application/json"})
r2=conn.getresponse()
data = r2.read()

print data

#print httpsget("cmsweb.cern.ch", "/reqmgr2/data/request?name=amaltaro_Run2015B-DoubleMuonLowMass-75XPreProd-22Jul2015_751_Harv_150826_131718_6174", {"Accept": "application/json"})