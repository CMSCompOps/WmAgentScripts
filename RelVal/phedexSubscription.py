#!/usr/bin/env python
import json
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import optparse



def TestAcceptedSubscritpionSpecialRequest(url, dataset, site):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&node='+site+'&type=xfer'+'&approval=approved')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	requests=result['phedex']
	if 'request' not in requests.keys():
		return False
	for request in result['phedex']['request']:
		for node in request['node']:
			if node['node']==site and node['decision']=='approved':
				return True
	return False



def TestSubscritpionSpecialRequest(url, dataset, site):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&node='+site+'&type=xfer')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	requests=result['phedex']
	if 'request' not in requests.keys():
		return False
	for request in result['phedex']['request']:
		for node in request['node']:
			if node['name']==site:
				return True
	return False

def TestCustodialSubscriptionRequested(url, dataset, site):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&node='+site+'_MSS')
	r2=conn.getresponse()
	result = json.loads(r2.read())
	requests=result['phedex']
	if 'request' not in requests.keys():
		return False
	for request in result['phedex']['request']:
		if request['approval']=='pending' or request['approval']=='approved':
			requestId=request['id']
			r1=conn.request("GET",'/phedex/datasvc/json/prod/transferrequests?request='+str(requestId))
			r2=conn.getresponse()
			result = json.loads(r2.read())
			if len(result['phedex']['request'])>0:
				requestSubscription=result['phedex']['request'][0]
			else:
				return False
			if requestSubscription['custodial']=='y':
				return True
	return False


#Changes the state of a workflow to closed-out
def closeOutWorkflow(url, workflowname):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #params = {"requestName" : workflowname,"status" : "announced"}	
    params = {"requestName" : workflowname, "cascade" : True}
    headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
    encodedParams = urllib.urlencode(params)
    conn.request("POST", "/reqmgr/reqMgr/closeout", encodedParams, headers)
    response = conn.getresponse()	
    if response.status != 200:
	    print "response status: "
	    print response.status
	    print "response reason: "
	    print response.reason
	    data = response.read()
	    print "response data: "
	    print data
	    print ""
    conn.close()

def announceWorkflow(url, workflowname):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #params = {"requestName" : workflowname,"status" : "announced"}	
    params = {"requestName" : workflowname,"status" : "announced"}
    headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
    encodedParams = urllib.urlencode(params)
    conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()	
    #print response.status, response.reason
    data = response.read()
    #print data
    conn.close()


def setWorkflowRunning(url, workflowname):
    print workflowname,
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    params = {"requestName" : workflowname,"status" : "running"}
    headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
    encodedParams = urllib.urlencode(params)
    conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()	
    print response.status, response.reason
    data = response.read()
    print data
    conn.close()

def abortWorkflow(url, workflowname):
    print workflowname,
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    params = {"requestName" : workflowname,"status" : "aborted"}
    headers={"Content-type": "application/x-www-form-urlencoded",
             "Accept": "text/plain"}
    encodedParams = urllib.urlencode(params)
    conn.request("PUT", "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()	
    print response.status, response.reason
    data = response.read()
    print data
    conn.close()

    
#Tests whether a dataset was subscribed to phedex
def testOutputDataset(datasetName):
	 url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/Data?dataset=' + datasetName
         result = json.loads(urllib2.urlopen(url))
	 dataset=result['phedex']['dbs']
	 if len(dataset)>0:
		return 1
	 else:
		return 0


#Test whether the output datasets for a workflow were subscribed
def testWorkflows(workflows):
	print "Testing the subscriptions, this process may take some time"
	for workflow in workflows:
		print "Testing workflow: "+workflow
		datasets=outputdatasetsWorkflow(workflow)
		numsubscribed=len(datasets)
		for dataset in datasets:
			if not testOutputDataset(dataset):
				print "Couldn't subscribe: "+ dataset
			else:
				numsubscribed=numsubscribed-1
		if numsubscribed==0:
			closeOutWorkflow(workflow)
			print "Everything subscribed and closedout"




#Return a list of outputdatasets for the workflows on the given list
def datasetforWorkfows(workflows):
	datasets=[]
	for workflow in workflows:
		datasets=datasets+outputdatasetsWorkflow(workflow)
	return datasets

#Return a list of workflows from the given file
def workflownamesfromFile(filename):
	workflows=[]
	f=open(filename,'r')
	for workflow in f:
		#This line is to remove the carrige return	
		workflow = workflow.rstrip('\n')
		workflows.append(workflow)
	return workflows	

#From a list of datasets return an XML of the datasets in the format required by Phedex
def createXML(datasets):
	# Create the minidom document
	impl=getDOMImplementation()
	doc=impl.createDocument(None, "data", None)
	result = doc.createElement("data")
	result.setAttribute('version', '2')
	# Create the <dbs> base element
	dbs = doc.createElement("dbs")
	dbs.setAttribute("name", "https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
	result.appendChild(dbs)	
	#Create each of the <dataset> element			
	for datasetname in datasets:
		dataset=doc.createElement("dataset")
		dataset.setAttribute("is-open","y")
		dataset.setAttribute("is-transient","y")
		dataset.setAttribute("name",datasetname)
		dbs.appendChild(dataset)
   	return result.toprettyxml(indent="  ")

#returns the output datasets for a given workfow
def outputdatasetsWorkflow(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName='+workflow)
	r2=conn.getresponse()
	datasets = json.loads(r2.read())
	while 'exception' in datasets:
		conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
		r1=conn.request("GET",'/reqmgr/reqMgr/outputDatasetsByRequestName?requestName='+workflow)
		r2=conn.getresponse()
		datasets = json.loads(r2.read())
	if len(datasets)==0:
		print "No Outpudatasets for this workflow: "+workflow
	return datasets
#Creates the connection to phedex
def createConnection(url):
	#key = "/afs/cern.ch/user/r/relval/.globus/amaltaro/userkey.pem"
	#cert = "/afs/cern.ch/user/r/relval/.globus/amaltaro/usercert.pem"
			 
	#conn = httplib.HTTPSConnection(url, key_file=key, cert_file=cert)
	conn =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	#conn =  httplib.HTTPSConnection(url, cert_file = "/home/relval/WmAgentScripts/RelVal/andrew_levin_proxy", key_file = "/home/relval/WmAgentScripts/RelVal/andrew_levin_proxy");
	conn.connect()
	return conn

# Create the parameters of the request
def createParams(site, datasetXML, comments):
	params = urllib.urlencode({ "node" : site+"_MSS","data" : datasetXML, "group": "DataOps", "priority":'normal', "custodial":"y","request_only":"y" ,"move":"y","no_mail":"n", "comments":comments})
	return params

def makeCustodialMoveRequest(url, site,datasets, comments):
	dataXML=createXML(datasets)
	params=createParams(site, dataXML, comments)
	conn=createConnection(url)
	conn.request("POST", "/phedex/datasvc/json/prod/subscribe", params)
	response = conn.getresponse()	
	#print response.status, response.reason
        #print response.read()

def makePhedexReplicaRequest(url, site,datasets, comments, custodial):	
	dataXML=createXML(datasets)
	if custodial:
		params = urllib.urlencode({ "node" : site,"data" : dataXML, "group": "RelVal", "priority":'normal', "custodial":"y","request_only":"y" ,"move":"n","no_mail":"n", "comments":comments})	
	else :
		params = urllib.urlencode({ "node" : site,"data" : dataXML, "group": "RelVal", "priority":'normal', "custodial":"n","request_only":"y" ,"move":"n","no_mail":"n", "comments":comments})
	conn=createConnection(url)
	conn.request("POST", "/phedex/datasvc/json/prod/subscribe", params)
	response = conn.getresponse()
        print 'Response from http call:'
	print 'Status:',response.status
	print 'Reason:',response.reason
	print 'Explanation:',response.read()

					


def main():
	parser = optparse.OptionParser()
	parser.add_option('--custodial', action="store_true", help='make a custodial subscription',dest='custodial')
	parser.add_option('--correct_env',action="store_true",dest='correct_env')
	(options,args) = parser.parse_args()

	command=""
	for arg in sys.argv:
		command=command+arg+" "

	if not options.correct_env:
		os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
		sys.exit(0)
		    
	if not len(args)==3:
		print "usage <site name> <name of file with datasets to subscribe> <comments> [--custodial]"
		sys.exit(0);

	site=args[0]
	filename=args[1]
	comments=args[2]
	url='cmsweb.cern.ch'
	datasets=[]
	f=open(filename,'r')
	for dataset in f:
		#This line is to remove the carrige return
		dataset = dataset.rstrip('\n')
		datasets.append(dataset)

	makePhedexReplicaRequest(url, site, datasets, comments,options.custodial);
	sys.exit(0);

if __name__ == "__main__":
	main()

