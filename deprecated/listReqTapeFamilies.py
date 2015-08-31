#!/usr/bin/env python
"""
@deprecated: No need to create tape families anymore 
"""
import urllib2, urllib, httplib, sys, re, os, json
from deprecated import phedexSubscription
from xml.dom.minidom import getDOMImplementation

def findCustodialLocation(url, dataset):
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        request=result['phedex']
        if 'block' not in request.keys():
                return "No Site"
        if len(request['block'])==0:
                return "No Site"
        for replica in request['block'][0]['replica']:
                if replica['custodial']=="y" and replica['node']!="T0_CH_CERN_MSS":
                        return replica['node']
        return "No Custodial Site found"

def getPrepID(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	prepID=request['PrepID']
	return prepID

def getInputDataSet(url, workflow):
	conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
	r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
	r2=conn.getresponse()
	request = json.loads(r2.read())
	inputDataSets=request['InputDataset']
	if len(inputDataSets)<1:
		print "No InputDataSet for workflow " +workflow
	else:
		return inputDataSets

def main():
	args=sys.argv[1:]
	if not len(args)==1:
		print "usage:listReqTapeFamilies.py filename" 
                print "where the file should contain a list of workflows"
		sys.exit(0)

        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC', 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL']

	filename=args[0]
	url='cmsweb.cern.ch'
        workflows=deprecated.phedexSubscription.workflownamesfromFile(filename)
        for workflow in workflows:
	   outputDataSets=deprecated.phedexSubscription.outputdatasetsWorkflow(url, workflow)
           prepID = getPrepID(url, workflow)
           ods = []

           # Set defaults & era
           lfn = '/store/mc'
           era = 'NONE'       

           if 'Summer12_DR52X' in prepID:
              ods = ['GEN-SIM-RECO', 'AODSIM', 'DQM']
              era = 'Summer12'
           if 'Summer12_DR53X' in prepID or 'Summer12DR53X' in prepID:
              ods = ['GEN-SIM-RECO', 'AODSIM', 'DQM']
              era = 'Summer12_DR53X'
           if 'Summer13dr53X' in prepID:
              ods = ['GEN-SIM-RECO', 'AODSIM', 'DQM']
              era = 'Summer13dr53X'
           if 'Summer11dr53X' in prepID:
              #ods = ['GEN-SIM-RECO', 'AODSIM', 'DQM']
              era = 'Summer11dr53X'
           if 'UpgFall13d' in prepID:
              era = 'UpgFall13d'
           if 'Fall13dr' in prepID:
              era = 'Fall13dr'
           if 'Summer11LegDR' in prepID:
              era = 'Summer11LegDR'
           if 'Spring14dr' in prepID:
              era = 'Spring14dr'
           if 'HiFall13DR53X' in prepID:
              era = 'HiFall13DR53X'
              lfn = '/store/himc'
	      
           if 'Fall11_R' in prepID or 'Fall11_HLTMuonia' in prepID or 'Fall11R' in prepID:
              ods = ['GEN-RAW', 'GEN-SIM-RECO', 'AODSIM', 'DQM']
              era = 'Fall11'
           if 'LowPU2010_DR' in prepID:
              era = 'Summer12'
           if 'UpgradeL1TDR_DR6X' in prepID:
              era = 'Summer12'

           if 'Winter13' in prepID or 'Winter13' in workflow:
              ods = ['GEN-SIM-RECO', 'AODSIM', 'DQM']
              era = 'HiWinter13'
              lfn = '/store/himc'

           if 'HiFall11DR44' in prepID:
               era = 'HiFall11'
               lfn = '/store/himc'

           if 'UpgradePhase' in workflow and ('DR61SLHCx' in workflow or 'dr61SLHCx' in workflow):
              era = 'Summer13'
              lfn = '/store/mc'

           # Check for any additionals, e.g. GEN-SIM-RECODEBUG
           for extra in outputDataSets:
              bits = extra.split('/')
              if bits[len(bits)-1] not in ods:
                 ods.append(bits[len(bits)-1])

           inputDataset = getInputDataSet(url, workflow)
           inputDatasetComps = inputDataset.split('/')

           # Determine site where workflow should be run
           count=0
           for site in sites:
              if site in workflow:
                 count=count+1
                 siteUse = site

           # Find custodial location of input dataset if workflow name contains no T1 site or multiple T1 sites
           if count==0 or count>1:
              siteUse = findCustodialLocation(url, inputDataset)
              siteUse = siteUse[:-4]

           # List required tape families and site name
           for od in ods:
              tapeFamily = lfn+'/'+era+'/'+inputDatasetComps[1]+'/'+od
              print tapeFamily,' ',siteUse

	sys.exit(0);

if __name__ == "__main__":
	main()

