#!/usr/bin/env python

import urllib
import urllib2
import base64
import httplib
import json
import sys
import socket
import time
import os
import urlparse

from HTMLParser import HTMLParser

def injectConfig(outputModuleNames):
    """
    _injectConfig_

    Inject a config into the config cache.  Return the ID.
    """
    configCacheDoc = {"config": None, "description": {"config_desc": "", "config_label": ""},
                      "info": None, "md5_hash": None, "owner": {"group": "Ops", "user": "sfoulkes"},
                      "pset_hash": None, "pset_tweak_details": {"process": {"parameters_": ["outputModules_"],
                                                                            "outputModules_": []}}}

    for outputModuleName in outputModuleNames:
        configCacheDoc["pset_tweak_details"]["process"]["outputModules_"].append(outputModuleName)
        outputModDict = {"fileName": "%s.root" % outputModuleName,
                         "dataset": {"parameters_": ["dataTier", "filterName"],
                                     "filterName": None,
                                     "dataTier": "RECO"},
                         "parameters_": ["fileName"]}
        configCacheDoc["pset_tweak_details"]["process"][outputModuleName] = outputModDict

    request = urllib2.Request("http://cms-xen41.fnal.gov:5984/wmagent_configcache",
                              json.dumps(configCacheDoc))
    base64Auth = base64.encodestring("dmwmwriter:purpl3Pants").replace("\n", "")
    request.add_header("Authorization", "Basic %s" % base64Auth)
    request.add_header("Content-Type", "application/json")
    couchResponseString = urllib2.urlopen(request).read()
    couchResponse = json.loads(couchResponseString)
    return couchResponse["id"]

class ReqMgrErrorParser(HTMLParser):
    inParagraph = False
    reqMgrError = None

    def handle_starttag(self, tag, attrs):
        if tag == "p":
            self.inParagraph = True

    def handle_data(self, data):
        if self.inParagraph:
            self.reqMgrError = data
            self.inParagraph = False
        
class ReqMgrInterface():
    reqMgrURL = None
    requestor = None
    verbose = False
    
    def __init__(self, reqMgrURL, verbose = False):
        self.verbose = verbose
        self.reqMgrURL = reqMgrURL
        self.requestor = None

        if self.verbose:
            print "Using: %s" % self.reqMgrURL
        return

    def createConnection(self, URL):
        """
        _createConnection_

        Create an httplib connection object for the given URL.
        """
        if URL.startswith("https://"):
            hostPort = URL.replace("https://", "")
            if "X509_USER_KEY" in os.environ.keys() and \
                   "X509_USER_CERT" in os.environ.keys():
                return httplib.HTTPSConnection(hostPort,
                                               key_file = os.environ["X509_USER_KEY"],
                                               cert_file = os.environ["X509_USER_CERT"])
            elif "X509_HOST_KEY" in os.environ.keys() and \
                   "X509_HOST_CERT" in os.environ.keys():
                return httplib.HTTPSConnection(hostPort,
                                               key_file = os.environ["X509_HOST_KEY"],
                                               cert_file = os.environ["X509_HOST_CERT"])
            else:
                print "Path to the key and cert files must be set in either"
                print "X509_HOST_[CERT|KEY] or X509_USER_[CERT|KEY]."
                sys.exit(-1)
        elif URL.startswith("http://"):
            hostPort = URL.replace("http://", "")
            return httplib.HTTPConnection(hostPort)
        else:
            print "URL must start with http:// or https://."
            sys.exit(-1)

        return None    
        
    def createRequest(self, reqParams):
        """
        _createRequest_

        Attempt to create a request in the ReqMgr.
        """
        reqHeaders = {"Content-type": "application/x-www-form-urlencoded",
                      "Accept": "application/json"}
        try:
            self.requestor = self.createConnection(self.reqMgrURL)            
            self.requestor.request("POST", "/reqmgr/create/makeSchema",
                                   urllib.urlencode(reqParams), reqHeaders)
        except socket.error, ex:
            if self.verbose:
                print "  Error connecting to ReqMgr: %s" % ex.strerror 
            sys.exit(-1)
            
        reqMgrResponse = self.requestor.getresponse()
        reqMgrResponseString = reqMgrResponse.read()        
        if reqMgrResponse.status != 200 and reqMgrResponse.status != 303:
            if self.verbose:
                print "  createRequest(): Creation failed, status: %d." % reqMgrResponse.status
                errorParser = ReqMgrErrorParser()
                errorParser.feed(reqMgrResponseString)
                if errorParser.reqMgrError != None:
                    print "  createRequest(): Reson: %s." % errorParser.reqMgrError
            return False
        else:
            for header in reqMgrResponse.getheaders():
                if header[0] == "location":
                    requestName = header[1].split('/')[-1]
                    if self.verbose:
                        print "  createRequest(): Succeeded, name: %s" % requestName
                    return requestName
            
        if self.verbose:
            print "  createRequest(): Failed, reson unknown."
        return False

    def assignRequest(self, requestName, teamName, acqEra, procVer, dashActivity,
                      siteWhitelist = [], siteBlacklist = [],
                      mergedLFNBase = "/store/data", unmergedLFNBase = "/store/unmerged",
                      minMergeSize = 2147483648, maxMergeSize = 4294967296,
                      maxMergeEvents = 50000, maxRSS = 2394967, maxVSize = 4294967296,
                      softTimeout = 171600, gracePeriod = 300):
        """
        _assignRequest_

        Attempt to assign a request in the ReqMgr.
        """
        reqParams = {"action": "Assign",
                     "Team" + teamName: "checked",
                     "SiteWhitelist": site,
                     "SiteBlacklist": [],
                     "MergedLFNBase": "/store/mc",
                     "UnmergedLFNBase": "/store/unmerged",
                     "MinMergeSize": 2147483648,
                     "MaxMergeSize": 4294967296,
                     "MaxMergeEvents": 50000,
                     "AcquisitionEra": era,
                     "ProcessingVersion": procversion,
                     "MaxRSS": 2394967,
                     "MaxVSize": 4294967296,
                     "Dashboard": activity,
                     "SoftTimeout":171600,
                     "GracePeriod":300,
                     "checkbox" + requestName: "checked"}

        
        return

    def approveRequest(self, requestName):
        """
        _approveRequest_

        Attempt to approve a request in the ReqMgr.
        """
        if self.verbose:
            print "  approveRequest(): Attempting to approve '%s'." % requestName
            
        # Check to see if the request exists and is in the correct state
        requestInfo = self.requestInfo(requestName)
        if requestInfo == None:
            return False
        elif requestInfo["RequestStatus"] != "new":
            if self.verbose:
                print "  approveRequest(): Request in wrong state '%s'." % requestInfo["RequestStatus"]
            return False

        reqParams = {"requestName": requestName, "status": "assignment-approved"}
        reqHeaders = {"Content-type": "application/x-www-form-urlencoded",
                      "Accept": "application/json"}
        try:
            self.requestor = self.createConnection(self.reqMgrURL)
            self.requestor.request("PUT", "/reqmgr/reqMgr/request",
                                   urllib.urlencode(reqParams), reqHeaders)
        except socket.error, ex:
            if self.verbose:
                print "  Error connecting to ReqMgr: %s" % ex.strerror 
            sys.exit(-1)
            
        reqMgrResponse = self.requestor.getresponse()
        if reqMgrResponse.status != 200:
            print "  approveRequest(): Approval failed, status: %d." % reqMgrResponse.status 
            return False
            
        reqMgrResponseString = reqMgrResponse.read()
        return True

    def changeRequestPriority(self, requestName, priority):
        return

    def abortRequest(self, requestName):
        """
        _abortRequest_

        Abort a request in the ReqMgr.
        """
        reqURL = "/reqmgr/reqMgr/request?requestName=%s&status=aborted" % requestName
        reqHeaders = {"Accept": "application/json"}

        try:
            self.requestor = self.createConnection(self.reqMgrURL)            
            self.requestor.request("PUT", reqURL, None, reqHeaders)
        except socket.error, ex:
            if self.verbose:
                print "  Error connecting to ReqMgr: %s" % ex.strerror 
            sys.exit(-1)
        
        reqMgrResponse = self.requestor.getresponse()
        reqMgrResponseString = reqMgrResponse.read()
        if reqMgrResponse.status == 404:
            if self.verbose:
                print "  requestInfo(): Request %s not found." % requestName
            return None

        print "Status: %s" % reqMgrResponse.status
        print "Response: %s" % reqMgrResponseString

        return True

    def requestInfo(self, requestName):
        """
        _requestInfo_

        Retrieve information about a request from the ReqMgr.
        """
        reqURL = "/reqmgr/reqMgr/request?requestName=%s" % requestName
        reqHeaders = {"Accept": "application/json"}

        try:
            self.requestor = self.createConnection(self.reqMgrURL)            
            self.requestor.request("GET", reqURL, None, reqHeaders)
        except socket.error, ex:
            if self.verbose:
                print "  Error connecting to ReqMgr: %s" % ex.strerror 
            sys.exit(-1)
        
        reqMgrResponse = self.requestor.getresponse()
        reqMgrResponseString = reqMgrResponse.read()
        if reqMgrResponse.status == 404:
            if self.verbose:
                print "  requestInfo(): Request %s not found." % requestName
            return None

        return json.loads(reqMgrResponseString)

    def retrieveSpec(self, requestName):
        """
        _retrieveSpec_

        Retrieve the pickled spec file from couch.
        """
        reqInfo = self.requestInfo(requestName)
        if reqInfo == None:
            return None

        specURL = reqInfo["RequestWorkflow"]
        urlComps = urlparse.urlparse(specURL)
        conn = self.createConnection(urlComps.scheme + "://" + urlComps.netloc)

        try:
            conn.request("GET", urlComps.path, None, {})
        except socket.error, ex:
            if self.verbose:
                print "  Error connecting to CouchDB: %s" % ex.strerror 
            sys.exit(-1)

        specResponse = conn.getresponse()
        specResponseString = specResponse.read()
        if specResponse.status == 404:
            if self.verbose:
                print "  retrieveSpec(): Request %s not found." % requestName
            return None

        return specResponseString

    def getRequestNames(self):
        """
        _getRequestNames_

        Retrieve a list of all the requests in the ReqMgr.
        """
        reqURL = "/reqmgr/reqMgr/requestnames"
        reqHeaders = {"Accept": "application/json"}

        try:
            self.requestor = self.createConnection(self.reqMgrURL)            
            self.requestor.request("GET", reqURL, None, reqHeaders)
        except socket.error, ex:
            if self.verbose:
                print "  Error connecting to ReqMgr: %s" % ex.strerror 
            sys.exit(-1)
        
        reqMgrResponse = self.requestor.getresponse()
        reqMgrResponseString = reqMgrResponse.read()
        return json.loads(reqMgrResponseString)

if __name__ == "__main__":
    int = ReqMgrInterface("http://cms-xen39.fnal.gov:8687", verbose = True)
    int.requestInfo("yourmom")
    int.approveRequest("yourmom")
    resp = int.requestInfo("sfoulkes_scfreq_120726_101315_9832")
    int.approveRequest("sfoulkes_scfreq_120726_101315_9832")

    int.createRequest(edgarLHEReq)
    configCacheID = injectConfig(["output"])
    edgarLHEReq["ProcConfigCacheID"] = configCacheID
    int.createRequest(edgarLHEReq)
