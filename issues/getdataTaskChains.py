"""
Retrieves Data from different sources and creates a single json file with
the request information about the TaskChains - Relvals and so
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

from WMCoreService.WMStatsClient import WMStatsClient
from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection, RequestInfo

#For reading overview
cachedoverview = '/afs/cern.ch/user/j/jbadillo/www/overview.cache'
afs_base = '/afs/cern.ch/user/j/jbadillo/www/'

#list of all requests
overview = []
allacdc = set()

#type of requests
REQ_TYPES = ['TaskChain']
REQ_TYPES_2 = ['Resubmission']

#interesting status
RUN_STATUS = ['assigned','acquired','failed','running-open','running-closed','completed']#'closed-out']
LIVE_STATUS = ['running','failed','running-open','running-closed','completed']#,'closed-out','announced']

#names of T1's
t2zone = {'T1_TW_ASGC':'ASGC','T1_IT_CNAF':'CNAF','T1_DE_KIT':'KIT','T1_FR_CCIN2P3':'IN2P3',
            'T1_ES_PIC':'PIC','T1_UK_RAL':'RAL','T1_US_FNAL':'FNAL'}

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
    

def loadJobSummary(rList):
    """
    Loads job summary from wmstats
    """
    #JSON job summary from WMStats view
    wMStats = WMStatsClient("https://cmsweb.cern.ch/couchdb/wmstats")
    jlist = {}
    print "Getting job information"
    for wf in rList:
        try:
            jdata = wMStats.getRequestByNames([wf], jobInfoFlag = True)
            print wf
            requestCol = RequestInfoCollection(jdata)
            for wf2, info in requestCol.getData().items():
                jlist[wf2] = {}
                for t,n in  info.getJobSummary().getJSONStatus().items():
                    jlist[wf2][t]=n
        except KeyError as e:
            print "ERROR, Cannot get job stats for", wf
            print e
        except Exception as e:
            print "ERROR, Cannot get job stats for", wf
            print e
    return jlist

def getWorkloadParameters(workflow):
    #Gets the workflow information from ReqMgr Workload
    batch = workflow.split('_')[2] #TODO fix this
    processingstring = workflow.split('_')[4] #TODO fix this

    # FIXME: broken code!
    list = reqMgr.getWorkflowWorkload('cmsweb.cern.ch', workflow)

    primaryds = ''
    cmssw = ''
    campaign = ''
    priority = -1
    timeev = 0
    sizeev = 0
    prepid = ''
    mergedLFNBase = ''
    globaltag = ''
    sites = []
    custodialsites = []
    blockswhitelist = []
    events_per_job = 0
    events_per_lumi = 0
    max_events_per_lumi = 0
    lumis_per_job = 0
    acquisitionEra = None
    processingVersion = None
    reqdate='' 
    requestdays = 0
    #Parse parameters from the workload
    for raw in list:
        if 'acquisitionEra' in raw:
            a = raw.find("'")
            if a >= 0:
                b = raw.find("'",a+1)
                acquisitionEra = raw[a+1:b]
            else:
                a = raw.find(" =")
                b = raw.find('<br')
                acquisitionEra = raw[a+3:b]
        elif 'primaryDataset' in raw:
            primaryds = raw[raw.find("'")+1:]
            primaryds = primaryds[0:primaryds.find("'")]
        elif '.schema.Campaign' in raw:
            campaign = raw[raw.find("'")+1:]
            campaign = campaign[0:campaign.find("'")]
        elif 'cmsswVersion' in raw:
            cmssw = raw[raw.find("'")+1:]
            cmssw = cmssw[0:cmssw.find("'")]
        elif 'properties.mergedLFNBase' in raw:
            mergedLFNBase = raw[raw.find("'")+1:]
            mergedLFNBase = mergedLFNBase[0:mergedLFNBase.find("'")]
        elif 'PrepID' in raw:
            prepid = raw[raw.find("'")+1:]
            prepid = prepid[0:prepid.find("'")]
        elif 'lumis_per_job' in raw:
            a = raw.find(" =")
            b = raw.find('<br')
            lumis_per_job = int(raw[a+3:b])
        elif '.events_per_job' in raw:
            a = raw.find(" =")
            b = raw.find('<br')
            events_per_job = float(raw[a+3:b])
        elif '.events_per_lumi' in raw:
            a = raw.find(" =")
            b = raw.find('<br')
            events_per_lumi = int(raw[a+3:b])
        elif '.max_events_per_lumi' in raw:
            a = raw.find(" =")
            b = raw.find('<br')
            max_events_per_lumi = int(raw[a+3:b])
        elif 'schema.SizePerEvent' in raw:
            a = raw.find("'")
            if a >= 0:
                b = raw.find("'",a+1)
                sizeev = int(raw[a+1:b])
            else:
                a = raw.find(" =")
                b = raw.find('<br')
                sizeev = int(float(raw[a+3:b]))
        elif 'schema.TimePerEvent' in raw:
            a = raw.find("'")
            if a >= 0:
                    b = raw.find("'",a+1)
                    timeev = int(raw[a+1:b])
            else:
                    a = raw.find(" =")
                    b = raw.find('<br')
                    timeev = int(float(raw[a+3:b]))
        elif 'request.priority' in raw:
            a = raw.find("'")
            if a >= 0:
                b = raw.find("'",a+1)
                priority = int(raw[a+1:b])
            else:
                a = raw.find(" =")
                b = raw.find('<br')
                priority = int(raw[a+3:b])
        elif 'schema.RequestDate' in raw:
            reqdate = raw[raw.find("[")+1:raw.find("]")]    
            reqdate = reqdate.replace("'","")
            reqdate= "datetime.datetime(" + reqdate + ")"
            reqdate= eval(reqdate)
            requestdays = (datetime.datetime.now()-reqdate).days
        elif 'blocks.white' in raw and not '[]' in raw:
            blockswhitelist = '['+raw[raw.find("[")+1:raw.find("]")]+']'    
            blockswhitelist = eval(blockswhitelist)        
        elif '.custodialSites' in raw and not '[]' in raw:
            custodialsites = '['+raw[raw.find("[")+1:raw.find("]")]+']'    
            custodialsites = eval(custodialsites)        
        elif 'sites.white' in raw and not '[]' in raw:
            sites = '['+raw[raw.find("[")+1:raw.find("]")]+']'    
            sites = eval(sites)        
        elif 'schema.processingVersion' in raw:
            processingVersion = raw[raw.find("'")+1:]
            processingVersion = processingVersion[0:processingVersion.find("'")]
            a = raw.find("'")
            if a >= 0:
                b = raw.find("'",a+1)
                processingVersion = raw[a+1:b]
            else:
                a = raw.find(" =")
                b = raw.find('<br')
                processingVersion = raw[a+3:b]
        elif 'request.schema.GlobalTag' in raw:
            globaltag = raw[raw.find("'")+1:]
            globaltag = globaltag[0:globaltag.find(":")]
    
    wlinfo = {'batch':batch,'processingstring':processingstring,'requestname':workflow,
            'primaryds':primaryds,'prepid':prepid,'globaltag':globaltag,'timeev':timeev,
            'sizeev':sizeev,'priority':priority,'sites':sites,'custodialsites':custodialsites,
            'acquisitionEra':acquisitionEra,'requestdays':requestdays,'reqdate':'%s' % reqdate,
            'processingVersion':processingVersion,'blockswhitelist':blockswhitelist,
            'events_per_job':events_per_job,'lumis_per_job':lumis_per_job,
            'max_events_per_lumi':max_events_per_lumi,'events_per_lumi':events_per_lumi,
            'campaign':campaign,'cmssw':cmssw,'mergedLFNBase':mergedLFNBase}
    return wlinfo

def getAssignmentDate(workflow):
    data = reqMgr.getWorkflowInfo('cmsweb.cern.ch', workflow)
    ls = data['RequestTransition']
    date = None
    for status in ls:
        if status['Status'] == 'assigned':
            #time in epoch format
            date = datetime.datetime.fromtimestamp(status['UpdateTime'])
            #parse to '%Y-%m-%d %H:%M:%S.%f')
            date = date.strftime('%Y-%m-%d %H:%M:%S.%f')
    return date

def getWorkflowInfo(workflow):
    """
    creates a single dictionary with all workflow information
    """
    
    wlinfo = getWorkloadParameters(workflow)
    timeev = wlinfo['timeev']
    sizeev = wlinfo['sizeev']
    prepid = wlinfo['prepid']
    sites = wlinfo['sites']
    custodialsites = wlinfo['custodialsites']
    events_per_job = wlinfo['events_per_job']
    lumis_per_job = wlinfo['lumis_per_job']
    blockswhitelist = wlinfo['blockswhitelist']

    #look for correspondin acdc's
    acdc = []
    for a in allacdc:
        if prepid in a:
            acdc.append(a)
    
    #retrieve reqMgr info
    s = reqMgr.getWorkflowInfo('cmsweb.cern.ch', workflow)
    
    #parse information
    filtereff = float(s['FilterEfficiency']) if 'FilterEfficiency' in s else 1
    team = s['Assignments'] if 'Assignments' in s else ''
    team = team[0] if type(team) is list and team else team
    typee = s['RequestType'] if 'RequestType' in s else ''
    status = s['RequestStatus'] if 'RequestStatus' in s else ''
    
    if 'RequestSizeEvents' in s:
        reqevts = s['RequestSizeEvents']
    elif 'RequestNumEvents' in s:
        reqevts = s['RequestNumEvents']
    
    inputdataset = {}
    if 'InputDatasets' in s and s['InputDatasets']:
        inputdataset['name'] = s['InputDatasets'][0]

    #TODO complete validation logic and expected run time
    inputdataset['phreqinfo'] = []
    inputdataset['phtrinfo'] = []
    
    ods = {}

    if workflow in jlist.keys():
        js = jlist[workflow]
    else:
        js = {}
    
    if status in LIVE_STATUS:
        updatedate = getAssignmentDate(workflow)
    else:
        updatedate = None
    wlinfo.update( {'filtereff':filtereff,'type':typee,'status':status,
                    'inputdatasetinfo':inputdataset,'timeev':timeev,'sizeev':sizeev,'sites':sites,
                    'js':js, 'team':team,'acdc':acdc,
                    'update':updatedate} )
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
    #read pledges from file
    dp = open(afs_base+'/pledged.json').read()
    pledged = json.loads(dp)
    
    #get acdc's and all requsts
    rList = getRequestsByTypeStatus(REQ_TYPES, RUN_STATUS)
    #allacdc = getRequestsByTypeStatus(['Resubmission'], RUN_STATUS)
    allacdc = []
    #job list
    jlist = loadJobSummary(rList)
    
    struct = []
    print "Number of requests in %s: %s\n" % (RUN_STATUS, len(rList))

    count = 1
    for workflow in rList:
        #try:
            #skip acdc's
            if workflow in allacdc:
                print "[skipping %s]\n" % (workflow)
                continue
            print "%s: %s" % (count, workflow)
            count += 1
            #get info        
            wfinfo = getWorkflowInfo(workflow)
            print " %s\n" % (wfinfo['update'])
            struct.append(wfinfo)
        #except Exception as e:
        #    print "ERROR: getting information for", workflow
        #    print e

    #write to file
    f = open(afs_base + 'data_taskchain.json','w')
    f.write(json.dumps(struct, indent=4, sort_keys=True))
    f.close()
    print "[END %s]\n" % (time.time() - now)

if __name__ == "__main__":
    main()

