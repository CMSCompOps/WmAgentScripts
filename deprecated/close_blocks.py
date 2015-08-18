#!/usr/bin/env python

#import simplejson as json
import urllib, httplib
import Queue
import json

from xml.dom.minidom import Document
import utils

q = Queue.Queue()

key = "/data/certs/servicekey.pem"
cert = "/data/certs/servicecert.pem"
file = "/afs/cern.ch/user/e/efajardo/scripts/cmsweb/WmAgentScripts/close.txt"

class Updater(object):
    def __init__(self):
        self.conn = httplib.HTTPSConnection("cmsweb.cern.ch", key_file=key, cert_file=cert)
        self.conn.connect()
        print "connected"

    def update(self, data, site = None):
        params = urllib.urlencode({"data":data, "node":site})
        self.conn.request("POST", "/phedex/datasvc/xml/prod/inject", params)
        response = self.conn.getresponse()
        print response.status, response.reason
        response.read()

    def close(self):
        self.conn.close()

def get_site(block):
    url = "https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod/data"
    params = {"block":block}
    data = json.loads(utils.download_data(url, params))["phedex"]
    return data["dbs"][0]["dataset"][0]["block"][0]["file"][0]["node"]

if __name__ == "__main__":
    updater = Updater()
    for block in open(file).readlines():
        block = block.strip()
        #site = get_site(block)
        site = "T1_US_FNAL_MSS"
        doc = Document()
        dt = doc.createElement("data")
        dt.setAttribute("version", "2.0")
        doc.appendChild(dt)
        
        dbs = doc.createElement("dbs")
        dbs.setAttribute("name", "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet")
        dt.appendChild(dbs)
        
        dataset = doc.createElement("dataset")
        dataset.setAttribute("name", block.split("#")[0].strip())
        dataset.setAttribute("is-open", "y")
        dbs.appendChild(dataset)

        bl = doc.createElement("block")
        bl.setAttribute("name", block)
        bl.setAttribute("is-open", "n")
        dataset.appendChild(bl)

        updater.update(doc.toxml(), site)

