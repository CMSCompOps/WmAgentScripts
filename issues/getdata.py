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

#from WMCoreService.WMStatsClient import WMStatsClient
#from WMCoreService.DataStruct.RequestInfoCollection import RequestInfoCollection, RequestInfo

#For reading overview
cachedoverview = '/afs/cern.ch/user/j/jbadillo/www/overview.cache'
afs_base = '/afs/cern.ch/user/j/jbadillo/www/'

#list of all requests
overview = []
allacdc = set()

#type of requests
rtype = ['ReDigi','MonteCarlo', 'MonteCarloFromGEN', 'Resubmission']
rtypeb = ['ReDigi','MonteCarlo', 'MonteCarloFromGEN']
#interesting status
rstatus = ['assignment-approved','assigned','acquired','running-open','running-closed','completed','closed-out']
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

def getRequestsByTypeStatus(rtype,rstatus):
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
    

def loadJobSummary():
    """
    Loads job summary from wmstats
    """
    #TODO JSON job summary from WMStats view (Seangchan API)
    """
    try:
        wMStats = WMStatsClient("https://cmsweb.cern.ch/couchdb/wmstats")
        jdata = wMStats.getRequestByNames(list, jobInfoFlag = True)
        requestCol = RequestInfoCollection(jdata)
        for wf, info in requestCol.getData().items():
            jlist[wf] = {}
            for t,n in  info.getJobSummary().getJSONStatus().items():
                jlist[wf][t]=n
    except:
        print "Cannot get job stats"
        pass
    #print jlist
    """
    return {}

def getWorkloadParameters(workflow):
    #Gets the workflow information from ReqMgr Workload
    batch = workflow.split('_')[2] #TODO fix this
    processingstring = workflow.split('_')[4] #TODO fix this
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
            events_per_job = int(raw[a+3:b])
        elif '.events_per_lumi' in raw:
            a = raw.find(" =")
            b = raw.find('<br')
            events_per_lumi = int(raw[a+3:b])
        elif '.max_events_per_lumi' in raw:
            a = raw.find(" =")
            b = raw.find('<br')
            max_events_per_lumi = int(raw[a+3:b])
        elif 'SizePerEvent' in raw:
            a = raw.find("'")
            if a >= 0:
                    b = raw.find("'",a+1)
                    sizeev = int(raw[a+1:b])
            else:
                    a = raw.find(" =")
                    b = raw.find('<br')
                    sizeev = int(float(raw[a+3:b]))
        elif 'TimePerEvent' in raw:
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
        elif 'RequestDate' in raw:
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
        elif 'processingVersion' in raw:
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


def getrequestupdates(req):
    updates = []
    for u in req['RequestUpdates']:
        #'update_time': '2014-08-22 14:45:41.871256'
        #print u
        date = datetime.datetime.strptime(u['update_time'], '%Y-%m-%d %H:%M:%S.%f')
        updates.append(date)
    if updates:
        update = min(updates)
        return update.strftime('%Y-%m-%d %H:%M:%S.%f')



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
    #check for one T1 that is in the whitelist
    custodialt1 = '?'
    for i in sites:
        if 'T1_' in i:
            custodialt1 = i
            break
    
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
        reqevts = 0
    
    inputdataset = {}
    if 'InputDatasets' in s and s['InputDatasets']:
        inputdataset['name'] = s['InputDatasets'][0]

    #TODO complete validation logic and expected run time
    
    if typee in ['MonteCarlo','LHEStepZero']:
    #if reqevts > 0:
        expectedevents = int(reqevts)
        if events_per_job > 0 and filtereff > 0:
            expectedjobs = int(math.ceil(expectedevents/(events_per_job*filtereff)))
            expectedjobcpuhours = int(timeev*(events_per_job*filtereff)/3600)
        else:
            expectedjobs = 0
            expectedjobcpuhours = 0
    elif typee in ['MonteCarloFromGEN','ReReco','ReDigi']:
        #datasets
        inputdataset['events'] = dbs3.getEventCountDataSet(inputdataset['name'])
        inputdataset['status'], inputdataset['createts'], inputdataset['lastmodts'] = dbs3.getDatasetInfo(inputdataset['name'])

        if blockswhitelist != []:
            inputdataset['bwevents'] = dbs3.getEventCountDataSetBlockList(inputdataset['name'],blockswhitelist)
        else:
            inputdataset['bwevents'] = inputdataset['events']

        if inputdataset['bwevents'] > 0 and filtereff > 0:
            expectedevents = int(filtereff*inputdataset['bwevents'])
        else:
            expectedevents = 0
        
        if events_per_job > 0 and filtereff > 0:
            expectedjobs = int(expectedevents/events_per_job)
        else:
            expectedjobs = 0
        
        try:
            expectedjobcpuhours = int(lumis_per_job*timeev*inputdataset['bwevents']/inputdataset['bwlumicount']/3600)
        except:
            expectedjobcpuhours = 0
        #TODO use phedexClient
        url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/RequestList?dataset=' + inputdataset['name']
        try:
            result = json.load(urllib.urlopen(url))
        except:
            print "Cannot get Requests List status from PhEDEx"
        try:
            r = result['phedex']['request']
        except:
            r = ''

        inputdataset['phreqinfo'] = []
        if r:
            for i in range(len(r)):
                phreqinfo = {}
                requested_by = r[i]['requested_by']
                nodes = []
                for j in range(len(r[i]['node'])):
                    nodes.append(r[i]['node'][j]['name'])
                id = r[i]['id']
                phreqinfo['nodes'] = nodes
                phreqinfo['id'] = id
                inputdataset['phreqinfo'].append(phreqinfo)
        
        url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/Subscriptions?dataset=' + inputdataset['name']
        try:
            result = json.load(urllib.urlopen(url))
        except:
            print "Cannot get Subscriptions from PhEDEx"
        inputdataset['phtrinfo'] = []
        try:
            print result
            rlist = result['phedex']['dataset'][0]['subscription']
            for r in rlist:
                phtrinfo = {}
                node = r['node']
                custodial = r['custodial']
                phtrinfo['node'] = node
                try:
                    phtrinfo['perc'] = int(float(r['percent_files']))
                except:
                    phtrinfo['perc'] = 0
                inputdataset['phtrinfo'].append(phtrinfo)
        except:
            r = {}

    else:
        expectedevents = -1
        expectedjobs = -1
        expectedjobcpuhours = -1
    
    expectedtotalsize = sizeev * expectedevents / 1000000
    conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=' + workflow)
    r2=conn.getresponse()
    data = r2.read()
    ods = json.loads(data)
    conn.close()
    if len(ods)==0:
        print "No Outpudatasets for this workflow: "+workflow
    outputdataset = []
    eventsdone = 0
    for o in ods:
        oel = {}
        oel['name'] = o
        if status in ['running','running-open','running-closed','completed','closed-out','announced']:
            #[oe,ost,ocreatets,olastmodts] = getdsdetail(o,timestamps=1)
            print "-",o, "-"
            oe = dbs3.getEventCountDataSet(o)
            ost, ocreatets, olastmodts = dbs3.getDatasetInfo(o)
            oel['events'] = oe
            oel['status'] = ost
            oel['createts'] = ocreatets
            oel['lastmodts'] = olastmodts
        
            phreqinfo = {}
            url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/Subscriptions?dataset=' + o
            try:
                result = json.load(urllib.urlopen(url))
            except:
                print "Cannot get subscription status from PhEDEx"
            try:
                r = result['phedex']['dataset']
            except:
                r = None
            if r:
                for i in range(0,len(r)):
                    approval = r[i]['approval']
                    requested_by = r[i]['requested_by']
                    custodialsite = r[i]['node'][0]['name']
                    id = r[i]['id']
                    if '_MSS' in custodialsite:
                        phreqinfo['custodialsite'] = custodialsite
                        phreqinfo['requested_by'] = requested_by
                        phreqinfo['approval'] = approval
                        phreqinfo['id'] = id
                oel['phreqinfo'] = phreqinfo
        
            phtrinfo = {}
            url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions?dataset=' + o
            try:
                result = json.load(urllib.urlopen(url))
            except:
                print "Cannot get transfer status from PhEDEx"
            try:
                rlist = result['phedex']['dataset'][0]['subscription']
            except:
                rlist = []
            
            phtrinfo = {}
            oel['phtrinfo'] = []
            for r in rlist:
                phtrinfo = {}
                node = r['node']
                custodial = r['custodial']
                if r['move'] == 'n':
                    phtype = 'Replica'
                else:
                    phtype = 'Move'
                phtrinfo['node'] = node
                phtrinfo['custodial'] = r['custodial']
                phtrinfo['time_create'] = datetime.datetime.fromtimestamp(int(r['time_create'])).ctime()
                phtrinfo['time_create_days'] = (datetime.datetime.now() - datetime.datetime.fromtimestamp(int(r['time_create']))).days
                try:
                    phtrinfo['perc'] = int(float(r['percent_files']))
                except:
                    phtrinfo['perc'] = 0
                phtrinfo['type'] = phtype

                oel['phtrinfo'].append(phtrinfo)
            eventsdone = eventsdone + oe
        else:
            eventsdone = 0
        outputdataset.append(oel)

    cpuhours = timeev*expectedevents/3600
    remainingcpuhours = max(0,timeev*(expectedevents-eventsdone)/3600)

    realremainingcpudays = 0
    totalslots = 0
    #pledge calculation
    for (psite,pslots) in pledged.items():
        if psite in sites:
            totalslots = totalslots + pslots
    if totalslots == 0:
        realremainingcpudays = 0
    else:
        realremainingcpudays = float(remainingcpuhours) / 24 / totalslots 
    try:
        zone = t2zone[custodialsites[0]]
    except:
        zone = '?'
    if workflow in jlist.keys():
        js = jlist[workflow]
    else:
        js = {}
    
    if status in ['running','running-open','running-closed','completed','closed-out','announced']:
        updatedate = getrequestupdates(s)
    else:
        updatedate = None
    wlinfo.update( {'filtereff':filtereff,'type':typee,'status':status,'expectedevents':expectedevents,
                    'inputdatasetinfo':inputdataset,'timeev':timeev,'sizeev':sizeev,'sites':sites,
                    'zone':zone,'js':js,'outputdatasetinfo':outputdataset,'cpuhours':cpuhours,
                    'realremainingcpudays':realremainingcpudays,'remainingcpuhours':remainingcpuhours,
                    'team':team,'expectedjobs':expectedjobs,'expectedjobcpuhours':expectedjobcpuhours,'acdc':acdc,
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
    dp=open(afs_base+'/pledged.json').read()
    pledged = json.loads(dp)
    
    #get acdc's and all requsts
    list = getRequestsByTypeStatus(rtype,rstatus)
    listb = getRequestsByTypeStatus(rtypeb,rstatus)
    allacdc = getRequestsByTypeStatus(['Resubmission'],rstatus)
    
    jlist=loadJobSummary()
    
    struct = []
    sys.stderr.write("Number of requests in %s: %s\n" % (rstatus,len(listb)))

    count = 1
    for workflow in list:
        try:
            #skip acdc's
            if workflow in allacdc:
                sys.stderr.write("[skipping %s]\n" % (workflow))
                continue
            sys.stderr.write("%s: %s" % (count,workflow))
            count += 1
            #get info        
            wfinfo = getWorkflowInfo(workflow)
            sys.stderr.write(" %s\n" % (wfinfo['update']))
            struct.append(wfinfo)
        except:
            print "error getting information for", workflow

    #write to file
    f = open(afs_base+'data.json','w')
    f.write(json.dumps(struct, indent=4, sort_keys=True))
    f.close()
    sys.stderr.write("[END %s]\n" % (time.time() - now))

if __name__ == "__main__":
    main()

