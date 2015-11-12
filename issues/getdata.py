"""
Retrieves Data from different sources and creates a single json file with
the request information about the ReDigi's
"""

import sys, os
import zlib, math, shutil
import urllib, urllib2 , re , time, datetime
import optparse, httplib
try:
    import json
except ImportError:
    import simplejson as json
from xml.dom import minidom

import WmAgentScripts.dbs3Client as dbs3
import WmAgentScripts.reqMgrClient as reqMgr
import WmAgentScripts.phedexClient as phd

from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection, RequestInfo

#For reading overview
cachedoverview = '/afs/cern.ch/user/j/jbadillo/www/overview.cache'
afs_base = '/afs/cern.ch/user/j/jbadillo/www/'

#list of all requests
overview = []
allacdc = set()

#type of requests
REQ_TYPES = ['ReDigi','MonteCarlo', 'MonteCarloFromGEN']
REQ_TYPES_2 = ['ReDigi','MonteCarlo', 'MonteCarloFromGEN', 'Resubmission', 'TaskChain']

#interesting status
RUN_STATUS = ['assignment-approved','assigned','acquired','running-open','running-closed','completed','closed-out']
LIVE_STATUS = ['running','running-open','running-closed','completed','closed-out','announced']

#names of T1's
t2zone = {'T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_DE_KIT':'KIT','T1_FR_CCIN2P3':'IN2P3',
            'T1_ES_PIC':'PIC','T1_UK_RAL':'RAL','T1_US_FNAL':'FNAL'}

DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
wMStats = WMStatsClient("https://cmsweb.cern.ch/couchdb/wmstats")

def getoverview():
    """
    Reads cached overview that contains a list of requests
    in the system.
    """
    #
    global cachedoverview
    if not os.path.exists(cachedoverview):
        print "Cannot get overview from %s" % cachedoverview
        sys.exit(1)
    d = open(cachedoverview).read()
    s = eval(d)
    return s

def getRequestsByTypeStatus(rtype, rstatus):
    """
    Filters only the requests on the given status and type from
    the overview
    """
    #set to avoid duplicates
    r = set()
    for wf in overview:
        if 'type' in wf.keys():
            t = wf['type']
        else:
            t = None
        if 'status' in wf.keys():
            st = wf['status']
        else:
            st = None
        if t in rtype and st in rstatus and t != '':
            #print "t = %s st = %s r = %s" % (t,st,i['request_name'])
            r.add(wf['request_name'])
    return r
    

def getJobSummary(workflow):
    """
    Loads job summary from wmstats
    """
    #JSON job summary from WMStats view
    jdata = {}
    try:
        jdata = wMStats.getRequestByNames([workflow], jobInfoFlag = True)
        requestCol = RequestInfoCollection(jdata)
        for wf2, info in requestCol.getData().items():
            jdata = info.getJobSummary().getJSONStatus()

    except KeyError as e:
        print "ERROR, Cannot get job stats for", workflow
        print e
        
    except Exception as e:
        print "ERROR, Cannot get job stats for", workflow
        print e

    return jdata

def getAssignmentDate(wfObject):
    ls = wfObject.cache['RequestTransition']
    date = None
    for status in ls:
        if status['Status'] == 'assigned':
            #time in epoch format
            date = datetime.datetime.fromtimestamp(status['UpdateTime'])
            #parse to '%Y-%m-%d %H:%M:%S.%f')
            date = date.strftime(DATE_FORMAT)
    return date

def getDatasetPhedexInfo(dataset):
    #request for subscriptions
    url='cmsweb.cern.ch'
    dataset['phreqinfo'] = []
    result = phd.phedexGet(url, '/phedex/datasvc/json/prod/requestlist?dataset='+dataset['name']+'&type=xfer')
    if 'request' not in result['phedex']:
        print "Cannot get Requests List status from PhEDEx"
    else:
        #aggregate results of all transfer requests
        for req in result['phedex']['request']:
            phreqinfo = {}
            requested_by = req['requested_by']
            phreqinfo['nodes'] = [ node['name'] for node in req['node'] ]
            phreqinfo['id'] = req['id']
            phreqinfo['approval'] = req['approval']
            #tape node
            for node in phreqinfo['nodes']:
                if node.endswith('_MSS'):
                    phreqinfo['custodialsite'] = node
            
            dataset['phreqinfo'].append(phreqinfo)
    
    #subscriptions
    dataset['phtrinfo'] = []
    
    result = phd.phedexGet(url, '/phedex/datasvc/json/prod/subscriptions?dataset='+dataset['name'])
    if 'dataset' not in result['phedex'] or not result['phedex']['dataset']:
        print "Cannot get Subscriptions from PhEDEx"
    else:
        #aggregate results of subscription
        for subs in result['phedex']['dataset'][0]['subscription']:
            phtrinfo = {}
            phtrinfo['custodial'] = subs['custodial']
            phtrinfo['node'] = subs['node']
            try:
                phtrinfo['perc'] = int(float(subs['percent_files']))
            except:
                phtrinfo['perc'] = 0
            phtrinfo['type'] = 'Move' if subs['move'] == 'y' else 'Replica'
            #time of request creating and days since
            time_create = datetime.datetime.fromtimestamp(int(subs['time_create']))
            phtrinfo['time_create'] = time_create.strftime(DATE_FORMAT)
            delta = time_create - datetime.datetime.now()
            phtrinfo['time_create_days'] = delta.days
            
            dataset['phtrinfo'].append(phtrinfo)

def getWorkflowInfo(workflow):
    """
    creates a single dictionary with all workflow information
    """
    #TODO replace all with info or cache
    wfObject = reqMgr.Workflow(workflow)
    #wlinfo = getWorkloadParameters(workflow)


    #Global stuff - common for all types of request
    try:
        wlinfo = {    # 'batch':batch  IGNORED - not useful
                #'processingstring': wfObject.info['ProcessingString'], IGNORED - not useful
                'requestname': workflow,
                'prepid': wfObject.info['PrepID'],
                'globaltag': wfObject.info['GlobalTag'],
                'timeev': wfObject.info['TimePerEvent'],
                'sizeev': wfObject.info['SizePerEvent'],
                'priority': wfObject.info['RequestPriority'],
                'sites': wfObject.info['SiteWhitelist'] if 'SiteWhitelist' in wfObject.info else None,
                'acquisitionEra':wfObject.info['AcquisitionEra'],
                'processingVersion': wfObject.info['ProcessingVersion'] if 'ProcessingVersion' in wfObject.info else 0,
                'campaign':wfObject.info['Campaign'],
                'cmssw':wfObject.info['CMSSWVersion'],
                'mergedLFNBase': wfObject.info['MergedLFNBase'] if 'MergedLFNBase' in wfObject.info else None,
                'type' : wfObject.type,
                'status':wfObject.status,
            }
    
        #get the following keys, or 0 by defaulr,
        getValue = lambda data, k, dflt = 0: data[k] if k in data else dflt; 
    
        #calculate general stuff
        #parse and format date
        now = datetime.datetime.now()
        dateArr = wfObject.info['RequestDate']
        #fill with missing zeros
        if len(dateArr) != 6:
            dateArr += (6-len(dateArr))*[0]
        reqdate = datetime.datetime.strptime(
                        "%s-%s-%s %s:%s:%s.0"%tuple(dateArr)
                        ,  DATE_FORMAT)
        wlinfo['reqdate'] = reqdate.strftime(DATE_FORMAT)
        #calculate days old of the request
        delta = now - reqdate
        days = delta.days + delta.seconds / 3600.0 / 24.0
        wlinfo['requestdays'] = days
        #assignment team and date
        if 'Assignments' in wfObject.info and wfObject.info['Assignments']:
            wlinfo['team'] = wfObject.info['Assignments'][0]
        else:
            wlinfo['team'] = ''
        wlinfo['update'] = getAssignmentDate(wfObject)
    
        wlinfo['expectedevents'] = getValue(wfObject.cache, 'TotalInputEvents')
        wlinfo['expectedjobs'] = getValue(wfObject.cache, 'TotalEstimatedJobs')
        
        #Job Information if available
        wlinfo['js'] = getJobSummary(workflow)
        
        #information about input dataset
        inputdataset = {}
        wlinfo['inputdatasetinfo'] = inputdataset
        if 'InputDatasets' in wfObject.info and wfObject.info['InputDatasets']:
            inputdataset['name'] = wfObject.info['InputDatasets'][0]
            
            wlinfo['blockswhitelist'] = getValue(wfObject.info, 'BlockWhitelist', None)
            if wlinfo['blockswhitelist']:
                wlinfo['blockswhitelist'] = eval(wlinfo['blockswhitelist'])
            
            inputdataset['events'] = dbs3.getEventCountDataSet(inputdataset['name'])
            dsinfo = dbs3.getDatasetInfo(inputdataset['name'])
            inputdataset['status'], wlinfo['inputdatasetinfo']['createts'], wlinfo['inputdatasetinfo']['lastmodts'] = dsinfo
        
            #block events
            if wlinfo['blockswhitelist']:
                inputdataset['bwevents'] = dbs3.getEventCountDataSetBlockList(inputdataset['name'], wlinfo['blockswhitelist'])
            else:
                inputdataset['bwevents'] = inputdataset['events']
            #load reqlist and subscriptions
            getDatasetPhedexInfo(inputdataset)
    
        #info about output datasets
        #expectedtotalsize = sizeev * expectedevents / 1000000
        outputdataset = []
        wlinfo['outputdatasetinfo'] = outputdataset
        eventsdone = 0
        if wfObject.status in ['running','running-open','running-closed','completed','closed-out','announced']:
            for o in wfObject.outputDatasets:
                oel = {}
                oel['name'] = o
                
                #[oe,ost,ocreatets,olastmodts] = getdsdetail(o,timestamps=1)
                print "-",o, "-"
                oel['events'] = wfObject.getOutputEvents(o)
                oel['status'], oel['createts'], oel['lastmodts'] = dbs3.getDatasetInfo(o)
                #load reqlist and subscriptions
                getDatasetPhedexInfo(oel)
    
                eventsdone = eventsdone + oel['events']
                outputdataset.append(oel)
    
    
        #look for correspondin acdc's
        wlinfo['acdc'] = []
        if wlinfo['prepid']:
            for a in allacdc:
                if wlinfo['prepid'] in a:
                    wlinfo['acdc'].append(a)
    
        if wfObject.type == 'TaskChain':
            pass
        #Stuff only for non-taskchain workflows
        else:
            wlinfo['primaryds'] =  wfObject.info['PrimaryDataset'],
    
            #get custodial sites from all output
            sites = []
            if 'SubscriptionInformation' in wfObject.info:
                for ds, info in wfObject.info['SubscriptionInformation'].items():
                   sites += info['CustodialSites']
            wlinfo['custodialsites'] = sites
            wlinfo['events_per_job'] = getValue(wfObject.info, 'EventsPerJob')
            wlinfo['lumis_per_job'] =  getValue(wfObject.info, 'LumisPerJob')
            wlinfo['events_per_lumi'] = getValue(wfObject.info, 'EventsPerLumi')
            wlinfo['max_events_per_lumi'] = getValue(wfObject.info, 'max_events_per_lumi')
            wlinfo['filtereff'] = getValue(wfObject.info, 'FilterEfficiency', 1.0)
            
            #calculate cpu hours        
            wlinfo['expectedjobcpuhours'] = wlinfo['timeev'] * wlinfo['expectedevents'] / wlinfo['filtereff']
        
    except Exception as e:
            print "Detail:", wfObject.info
            raise e
    return wlinfo

def main():
    global overview, forceoverview, pledged, allacdc, jlist
    now = time.time()
    """
    output=os.popen("ps aux | grep getdata | grep -v grep").read().split('\n')
    #Verify that the same process is not running
    if len(output)>2:
        print "Process already running:"
        print '\n'.join(output)
        sys.exit(0)
    """
    #read overview from file
    overview = getoverview()
    
    #get acdc's and all requsts
    wfs = getRequestsByTypeStatus(REQ_TYPES_2, RUN_STATUS)
    allacdc = getRequestsByTypeStatus(['Resubmission'], RUN_STATUS)
    
    struct = []
    print "Number of requests in %s: %s\n" % (RUN_STATUS, len(wfs))

    count = 1
    for workflow in wfs:
        count += 1
        try:
            #skip acdc's
            if workflow in allacdc:
                print "[skipping %s]\n" % (workflow)
                continue
            #skip relvals
            if "RVCMSSW" in workflow or "RelVal" in workflow:
                print "[skipping %s]\n" % (workflow)
                continue
            
            print "%s: %s" % (count, workflow)
            
            #get info        
            wfinfo = getWorkflowInfo(workflow)
            print " %s\n" % (wfinfo['update'])
            struct.append(wfinfo)
        except Exception as e:
            print e
            print "error getting information for", workflow

    #write to file
    f = open(afs_base + 'data.json','w')
    f.write(json.dumps(struct, indent=4, sort_keys=True))
    f.close()
    print "[END %s]\n" % (time.time() - now)

if __name__ == "__main__":
    main()

