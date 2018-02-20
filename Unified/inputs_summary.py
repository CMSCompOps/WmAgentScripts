from assignSession import *
import time
from utils import getWorkLoad, checkTransferStatus, workflowInfo, getWorkflowById, makeDeleteRequest, getWorkflowByOutput, getDatasetPresence, updateSubscription, getWorkflowByInput, getDatasetBlocksFraction, siteInfo, getDatasetDestinations, getSiteWhiteList, check_ggus, getDatasetEventsPerLumi, getWorkflowByMCPileup, getDatasetStatus, getDatasetBlocks, checkTransferLag, listCustodial, listRequests, getSubscriptions, makeReplicaRequest,getDatasetEventsAndLumis, getLFNbase, getDatasetFiles, getDatasetBlockSize, getWorkflowById
from reqMgrClient import retrieveSchema
import pprint
import sys
import json
import itertools 
import copy
from collections import defaultdict
from utils import lockInfo, closeoutInfo
import phedexClient
import reqMgrClient
import dbs3Client
import math
import random

from utils import check_ggus

from utils import siteInfo
from htmlor import htmlor
from utils import dataCache , DbsApi
from utils import findLostBlocks, findCustodialLocation, findCustodialCompletion, checkDownTime, getWorkflowByMCPileup, getDatasetSize, getDatasetBlockAndSite
from utils import GET, getWorkflows, getDatasetBlockFraction, findLostBlocksFiles, getDatasetFileFraction, DSS
from utils import closeoutInfo, findLateFiles, listRequests, getDatasetLumis, sendLog, searchLog, campaignInfo, try_sendLog, getDatasetFiles
import itertools
import httplib 
from utils import setDatasetStatus, getDatasetEventsPerLumi, monitor_dir, getUnsubscribedBlocks, base_dir, distributeToSites,getDatasetChops, getDatasetFileLocations, getAllAgents, getDatasetFileLocations
url = 'cmsweb.cern.ch'


all_checks = {}
check_those = []
for status in [
    'considered',
    'staging',
    'away',
    ]:
    all_checks[status] = session.query(Workflow).filter(Workflow.status.startswith(status)).all()
    check_those.extend( all_checks[status] )


check_those = filter(lambda wfo : 'RunIISummer16DR80' in wfo.name, check_those )
check_those = filter(lambda wfo : 'RunIISummer16DR80Premix' in wfo.name, check_those )




if sys.argv[1] == 'pertape':
    ## in GB/s
    rate = { 
        'T1_US_FNAL_MSS' : 0.4,
        'T0_CH_CERN_MSS' : 0.5,
        'T1_IT_CNAF_MSS' : 0.4,
        'T1_ES_PIC_MSS' : 0.3,
        'T1_DE_KIT_MSS' : 0.2,
        'T1_RU_JINR_MSS' : 0.1,
        'T1_FR_CCIN2P3_MSS' : 0.2,
        'T1_UK_RAL_MSS' : 0.3
        }
    #normalize to 2
    total_rate = sum(rate.values())
    correction = 2./total_rate
    for s in rate: rate[s] *= correction

    blob = json.loads( open('%s/inputs.json'%monitor_dir).read())
    print len(blob)
    by_prio = defaultdict(dict)
    wf_names = set([wfo.name for wfo in check_those])
    really_counted = 0
    for wfo in check_those:
        wf = wfo.name
        if not wf in blob:continue
        if wf not in wf_names: continue

        really_counted +=1
        i = blob[wf]
        #for wf,i in blob.iteritems():

        ii = copy.deepcopy(i)
        wfo = filter(lambda o:o.name==wf, check_those)[0]

        #fraction = min(1,i['available'])
        fraction = max([f for s,(t,f) in i['on_disk'].items()]) / 100. if i['on_disk'] else 0.

        if wfo.status == 'away': fraction = 1.

        fraction = min(fraction,1.)
        ii.update({'workflow': wf,
                   'staged' : float(i['size']*fraction),
                   'staging' : float(i['size']*(1.-fraction)),
                   #'staged' : float(i['size']*min(1,i['available'])),
                   #'staging' : float(i['size']*max(0,(1.-i['available']))),
                   'status' : wfo.status,
                   'wmstatus' : wfo.wm_status,
                   'priority' : i['priority'],
                   'fraction' : fraction
                   })
        by_prio[i['priority']].update( {wf: ii} )

    i_on_tape = {}
    i_on_disk = {}
    tape_delay = {}
    top=20
    all_top_10 = {}
    l_all_top_10 = {}
    for prio in sorted(by_prio.keys(), key = lambda o:int(o)):
        print prio
        by_site = defaultdict(dict)
        for wf,i in by_prio[prio].iteritems():
            #print i['on_tape']
            on_tape = i['on_tape'].keys()
            if on_tape:
                by_site[on_tape[0]].update( {wf:i} )
            else:
                print i['input'],"not on tape"

        sum_by_site ={}
        disk_by_site = {}
        delay_by_site = {}
        top_10 = {}
        l_top_10 = {}
        for site in by_site:
            #sum_by_site[site] = int(sum([o['size']*(1.-o['available']) for wf,o in by_site[site].iteritems()]))
            #disk_by_site[site] = int(sum([o['size']*(o['available']) for wf,o in by_site[site].iteritems()]))  
            sum_by_site[site] = int(sum([o['staging'] for wf,o in by_site[site].iteritems()]))
            disk_by_site[site] = int(sum([o['staged'] for wf,o in by_site[site].iteritems()]))  
            top_10[site] = dict(sorted([(wf,dict([(kk,vv) for kk,vv in o.items() if kk in ['workflow','staged','staging','status','wm_status','priority','fraction','available']])) for (wf,o) in by_site[site].iteritems()], key = lambda k : k[1]['staging'],reverse=True)[:top])
            print [o['staging'] for o in top_10[site].values()]
            if site in rate:
                ## in days
                delay_by_site[site] = '%.2f'%((sum_by_site[site] / float(rate[site])) / (60*60*24) )
        print "in GB"
        print json.dumps( sum_by_site , indent=2)
        i_on_tape[prio] = sum_by_site
        print "in GB"
        print json.dumps( disk_by_site, indent=2) 
        i_on_disk[prio] = disk_by_site
        print "in days"
        print json.dumps( delay_by_site , indent=2)        
        tape_delay[prio] = delay_by_site
        all_top_10[prio] = top_10
        print "worse input"
        for site in top_10:
            l_top_10[site] = sorted([o for wf,o in top_10[site].items() ],key = lambda o : o['staging'], reverse=True)
        l_all_top_10[prio] = l_top_10

    total_tape = defaultdict(int)
    for prio in i_on_tape:
        for site in i_on_tape[prio]:
            total_tape[site] += i_on_tape[prio][site]
    total_staged = defaultdict(int)

    for prio in i_on_disk:
        for site in i_on_disk[prio]:
            total_staged[site] += i_on_disk[prio][site]

    total_delay = defaultdict(float)
    for prio in tape_delay:
        for site in tape_delay[prio]:
            total_delay[site] += float(tape_delay[prio][site])
    for site in total_delay:
        total_delay[site] = '%.2f'% total_delay[site]

    top_top_10 = defaultdict(dict)
    l_top_top_10 = defaultdict(dict)
    for prio in all_top_10:
        for site in all_top_10[prio]:
            top_top_10[site].update( all_top_10[prio][site])

    for site in top_top_10:
        top_top_10[site] = dict(sorted([(wf,o) for wf,o in top_top_10[site].items()], key = lambda k : k[1]['staging'], reverse=True)[:top])
        l_top_top_10[site] = sorted([o for wf,o in top_top_10[site].items() ],key = lambda o : o['staging'], reverse=True)


    now = time.asctime(time.gmtime())
    o = open('%s/inputs_summary.txt'%monitor_dir,'w')
    o.write( "updated %s GMT \n"% now )
    #if 'still_working' in blob:
    #    o.write( "Complete update on %s GMT"% blob['still_working'])
    
    o.write( "%d workflow considered so far out of %d\n"%( really_counted, len(check_those)))
    o.write( "#"*40+"\n" )
    o.write( "Average rate considered : total %.3f GB/s \n%s\n" % (sum(rate.values()),json.dumps( rate, indent=2)))
    o.write( "#"*40+"\n" )
    o.write( "Total input already on disk in GB : %d \n%s\n"%(sum(total_staged.values()), json.dumps( total_staged, indent=2)))
    o.write( "Total input still on tape in GB : %d \n%s\n"%(sum(total_tape.values()),  json.dumps( total_tape, indent=2)))
    o.write( "Approximate total retrieval time in days : max %.2f days \n%s\n"%(max(map(float,total_delay.values())), json.dumps( total_delay , indent=2)))
    o.write( "#"*40+"\n" )
    #o.write( "Top 10 Worse workflows\n%s\n"% json.dumps( top_top_10, indent=2))
    o.write( "Top %d Worst workflows (worst on top)\n%s\n"%(top, json.dumps(l_top_top_10, indent=2)))
    o.write( "#"*40+"\n" )
    o.write( "Total input already on disk per priority in GB\n%s\n"% json.dumps( i_on_disk , indent=2))
    o.write( "Total input still on tape per priority in GB\n%s\n"% json.dumps( i_on_tape , indent=2))
    o.write( "Approximate retrieval time in days\n%s\n"% json.dumps( tape_delay , indent=2))
    o.write( "#"*40+"\n" )
    #o.write( "Top 10 Worse workflows\n%s\n"% json.dumps( all_top_10, indent=2))
    o.write( "Top %d Worst workflows (worst on top)\n%s\n"%(top, json.dumps( l_all_top_10, indent=2)))


    o.close()
    fdasfd

print len(check_those),"to be analyzed"

random.shuffle( check_those )
#check_those = check_those[:int(sys.argv[1])]

SI = siteInfo()
mcores = [SI.CE_to_SE(site) for site in SI.sites_mcore_ready]

#all_info=defaultdict(dict)
all_info = json.loads(open('%s/inputs.json'%monitor_dir).read())

checked=0

redone=False
redo=True


if not redo:
    check_those = [wfo for wfo in check_those if wfo.name not in all_info.keys()]

print len(check_those),"to look at"
#print [wfo.name for wfo in check_those][:10]

now = time.mktime(time.gmtime())

delay=2*24*60*60

for wfo in check_those:
    if wfo.name in all_info: 
        if 'updatded' in all_info[wfo.name] and (now-all_info[wfo.name]['updated'])< delay:
            print "too fresh"
            continue
            
        if redo:
            print "redoing",wfo.name
            redone=True
        else:
            #print wfo.name,"done already"
            continue
    else:
        print "new",wfo.name

    checked+=1
    if checked > int(sys.argv[1]):
        print "enough checked",checked
        break
    print wfo.name
    wfi = workflowInfo(url , wfo.name )
    prio = str(wfi.request['RequestPriority'])
    #if not prio in all_info: all_info[prio] = {}

 
    _,prim,_,_ = wfi.getIO()
    dataset = list(prim)[0]
    presence = getDatasetPresence(url, dataset, vetoes=[])
    usable = dict([(k,v) for (k,v) in presence.items() if k in mcores])
    tape = dict([(k,v) for (k,v) in presence.items() if 'MSS' in k])
    available = getDatasetBlocksFraction(url, dataset, sites = mcores)
    size = getDatasetSize( dataset )
    print dataset,prio,available,size
    #print json.dumps( usable, indent=2)
    #print tape.keys()

    all_info[wfo.name] = { 'input' : dataset,
                           'priority' : prio,
                           'available' : available,
                           'on_disk' : usable,
                           'on_tape' : tape,
                           'size' : size,
                           'updated' : now
                           }


if not redone:
    ## nothing has been touched this round
    now = time.asctime(time.gmtime())
    all_info['still_working'] = now

open('%s/inputs.json'%monitor_dir,'w').write( json.dumps(all_info, indent=2))

fdsgf

#for wfo in session.query(Workflow).filter(Workflow.status == 'staging').filter(Workflow.name.contains('2015C_25')).all():
#    print wfo.name,wfo.status
#    wfo.status = 'considered'  
#session.commit()




#print "\n".join(d)



"""
agents = defaultdict(lambda : defaultdict(int))
wfs =  session.query(Workflow).filter(Workflow.status == 'away').all()
random.shuffle( wfs ) 
for iwf,wfo in enumerate(wfs):
    wfi = workflowInfo(url, wfo.name)
    for a,ss in wfi.getAgents().items():
        for s,c in ss.items():
            agents[a][s] += c
    print iwf,len( wfs), wfo.name
    
    if iwf%50==0:
        print json.dumps( agents, indent=2 )

print json.dumps( agents, indent=2 )
"""


"""
wfi = workflowInfo(url, "pdmvserv_task_HIG-RunIIFall15DR76-00026__v1_T_151121_195339_3945")

wq = wfi.getWorkQueue()
wqes = [w[w['type']] for w in wq]


wq_running = [wqe for wqe in wqes if wqe['Status'] == 'Running']

active_agents= defaultdict(int)
for wqe in wq_running: active_agents[wqe['ChildQueueUrl']]+=1
wq_done = [wqe for wqe in wqes if wqe['Status'] == 'Done']
done_agents = defaultdict(int)
for wqe in wq_done: done_agents[wqe['ChildQueueUrl']]+=1

print len(wq_running), len(wq_done)
print active_agents
print done_agents
"""


"""
for wfo in session.query(Workflow).filter(Workflow.status == 'away').all():
    if wfo.wm_status == 'normal-archived':
        print wfo.name
        wfo.status = 'done'
session.commit()
   
"""
     
"""
pid = 549566
checks = checkTransferStatus(url, pid, nocollapse=True)
print json.dumps( checks )
"""

"""
output='/SLQ_Rhanded-MLQ600g1r0p2_13TeV-calchep/RunIIFall15DR76-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/AODSIM'

event_count,lumi_count = getDatasetEventsAndLumis(dataset=output)
print event_count,lumi_count
"""

#prim = '/JetHT/Run2015D-v1/RAW'
#tapes = [site for site in getDatasetPresence( url, prim, vetoes=['T0','T3','T2','Disk']) if site.endswith('MSS')]

#print tapes




"""
si = siteInfo()
for s in si.sites_ready:
    if s in si.sites_pressure:
        (m, r, pressure) = si.sites_pressure[s]
        if float(m) < float(r):
            print "\t",s,m,r,"lacking pressure"
        else:
            print s,m,r,"pressure"
"""

#rs = getWorkflows(url, status='running-closed')
#rs.extend( getWorkflows(url, status='running-open'))

"""
for wfo in session.query(Workflow).filter(Workflow.status.startswith('assistance')).all():
    if wfo.wm_status.startswith('running'):
    print wfo.name,wfo.wm_status
        wfo.status ='away'
        session.commit()
    else:
        wfi = workflowInfo(url, wfo.name)
        if wfi.request['RequestStatus'].startswith('running'):
            print wfo.name,wfo.wm_status
            wfo.status ='away'
            session.commit()
"""           
#for wfn in json.loads(open('/afs/cern.ch/user/v/vlimant/public/ops/bypass.json').read()):
#    wfo = session.query(Workflow).filter(Workflow.name == wfn).first()
#    print wfo.status
#    #wfo.status = 'away'
##session.commit()

"""
dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
bads=set()
for o in session.query(Output).filter(Output.datasetname.contains('-v4')).all():
    reply = dbsapi.listFiles(logical_file_name='/store/data*',dataset=o.datasetname)
    if len(reply):
        print o.datasetname,len(reply)
        bads.update(getWorkflowByOutput(url, o.datasetname))

print list(bads)
"""

#print getLFNbase('/DoubleEG/Run2015D-v1/RAW')

"""
si = siteInfo()

s=0
for site in set(si.sites_T1s + si.sites_with_goodIO):
    s += si.cpu_pledges[site]
    summary = json.loads(os.popen('curl -s http://cms-gwmsmon.cern.ch/prodview/json/%s/summary'%site).read())
    for wf in summary:
        pass
    ## measure the pressure and what it is running
print s
"""



#for ds in filter(None,os.popen("grep size ../logs/lockor/2015-12-09_07:45:38.log  | awk '{ print $3}'").read().split('\n')):
#    findCustodialCompletion(url, ds )
#print findCustodialCompletion(url, '/WJetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIIWinter15GS-MCRUN2_71_V1_ext1-v1/GEN-SIM')
#print findCustodialCompletion(url, '/TT_TuneCUETP8M1_13TeV-powheg-pythia8/RunIISummer15GS-MCRUN2_71_V1_ext3-v1/GEN-SIM')
#print findCustodialCompletion(url, '/WJetsToLNu_BGenFilter_Wpt-40toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM')

#locks = set(json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/globallocks.json').read()))
#print len(locks)
#locks = filter(lambda ds : not '-v0/' in ds, locks)
#print len(locks)
#open('/afs/cern.ch/user/c/cmst2/www/unified/globallocks.json','w').write( json.dumps( list(locks), indent=2))

#for wfo in session.query(Workflow).filter(Workflow.status == 'considered-unlock').all():
#    wfo.status = 'considered'
#session.commit()

#sys.exit(2)
"""
all_datasets=[]
item = GET(url, '/phedex/datasvc/json/prod/transferrequests?request=%s'% 525772) ['phedex']['request'][0]
#print type( item['data']['dbs']['dataset'])
for ds in item['data']['dbs']['dataset']:
    #print ds['name']
    if ds['name'] in ['/BlackHole_BH2_MD-2000_MBH-9000_n-2_TuneCUETP8M1_13TeV-blackmax/RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/MINIAODSIM']: 
        print "SKIP",ds['name']
        continue
    all_datasets.append( ds['name'] )

print len(all_datasets)
s=0
while all_datasets:
    those = all_datasets[:40]
    #print all_datasets[40]
    all_datasets = all_datasets[40:]
    #print those[-1]
    #print all_datasets[0]
    s += len(those)
    print len(those)
    makeReplicaRequest(url, 'T1_IT_CNAF_MSS', those, comments='replacing https://cmsweb.cern.ch/phedex/prod/Request::View?request=525772', priority='high', custodial='y')
    time.sleep(2)
print s
"""

#for wfo in session.query(Workflow).filter(Workflow.name=='fabozzi_Run2015D-DoubleEG-03Dec2015_7415p1_151203_165123_4976').all():
#    print wfo.id,wfo.name,wfo.status

#wfo = session.query(Workflow).get(29024)
#print wfo.status
#session.delete(wfo)
#session.commit()


#print getWorkflows(url, "assignment-approved", details=True)#, rtype="ReReco", details=True, user='pdmvserv,')

#print getDatasetEventsAndLumis('/SMS-T2tt_mStop-600-950_mLSP-1to450_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIIWinter15pLHE-MCRUN2_71_V1-v1/LHE')
#print getDatasetEventsAndLumis('/SMS-T2tt_mStop-600-950_mLSP-1to450_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15FSPremix-FastAsympt25ns_MCRUN2_74_V9-v1/MINIAODSIM')

"""


for wfo in session.query(Workflow).filter(Workflow.status=='staging').all():
    wfi = workflowInfo(url, wfo.name )
    if wfi.request['RequestPriority'] < 100000 : continue
    os.system('Unified/assignor.py --go %s'% wfo.name)

"""

"""

#nl = newLockInfo()
delete_site = set()
delete_pu = set()
for wfo in session.query(Workflow).filter(Workflow.status=='away').all():
    those = ['Summer12DR53' , 'Summer11LegDR', 'RunIISpring15DR74']
    if not any([d in wfo.name for d in those]): continue
    wfi = workflowInfo(url, wfo.name )
    #if wfi.request['RequestPriority'] > 90000: continue

    (_,_,_,s) = wfi.getIO()
    ## collect the site/pu to be deleted
    running_at = wfi.request['SiteWhitelist']
    
    good = [ "T1_US_FNAL", "T1_ES_PIC", "T1_FR_CCIN2P3","T1_IT_CNAF","T1_RU_JINR","T1_UK_RAL","T1_DE_KIT","T2_CH_CERN"]
    if all([d in good for d in running_at]):
        print wfo.name,"is ok to stay alive"
        continue

    delete_site.update( set(running_at) - set(good) )

    delete_pu.update( s )
    
    ## need to kill and clone
    print "taking out",wfo.name
    os.system('Unified/rejector.py --clone %s'% wfo.name)

#delete_site.remove('T2_CH_CERN')
print delete_site
print delete_pu
#makeDeleteRequest(url, list( delete_site ) , list(delete_pu), "closing some campaigns on T2s")

"""



#c = listCustodial(url)
#print json.dumps( c, indent=1)
#oldest = min([min(v) for v in c.values()])
#s=0
#for site in c:
#    for pid in c[site]:
#        d=GET(url,'/phedex/datasvc/json/prod/transferrequests?request=%d'%pid)
#        a_s = sum( [i['data']['bytes'] for i in d['phedex']['request']])
#        print pid,a_s
#        s+=a_s
#print s


#wfi = workflowInfo(url, 'pdmvserv_BPH-Summer12DR53X-00191_00424_v0__151105_132939_5147')

#print wfi.getSplittings()[0]

"""

waiting = filter(None, os.popen("grep size ../logs/lockor/last.log  | awk '{print $3}'").read().split('\n'))
si=siteInfo()


#by_input = GET(url, '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byinputdataset?&include_docs=true')["rows"]
#by_output = GET(url, '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/byoutputdataset?&include_docs=true')["rows"]
#by_pileup = GET(url, '/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bymcpileup?&include_docs=true')["rows"]

"""


"""
for transfer in session.query(Transfer).filter(Transfer.phedexid>0).all()[:500]:
    skip = True
    for wfid in transfer.workflows_id:
        tr_wf = session.query(Workflow).get(wfid)
        if tr_wf:
            if tr_wf.status == 'staging':
                print "\t",transfer.phedexid,"is staging for",tr_wf.name
                skip=False

    if skip:
        #could be deleted IMO
        print transfer.id, transfer.phedexid, len(transfer.workflows_id)
        transfer.phedexid = -transfer.phedexid
session.commit()
"""

#print checkDownTime()

#si = siteInfo()
#for t in si.storage:
#    print t,si.storage[t]

#wfi = workflowInfo(url, 'pdmvserv_task_SUS-RunIIWinter15wmLHE-00097__v1_T_151012_202658_9615')
#print workflowInfo(url, 'pdmvserv_task_TOP-RunIIWinter15wmLHE-00044__v1_T_151026_123856_4081').getSiteWhiteList()
#print workflowInfo(url, 'pdmvserv_task_SUS-RunIIWinter15wmLHE-00097__v1_T_151012_202658_9615').getSiteWhiteList()
#print workflowInfo(url, 'pdmvserv_task_TOP-RunIIWinter15wmLHE-00038__v1_T_151030_212808_6349').getSiteWhiteList()
#print workflowInfo(url, 'pdmvserv_task_TOP-RunIIWinter15wmLHE-00040__v1_T_151030_212845_1146').getSiteWhiteList()
#print workflowInfo(url, 'pdmvserv_task_TOP-RunIIWinter15wmLHE-00039__v1_T_151030_212843_9829').getSiteWhiteList()
#print workflowInfo(url, 'pdmvserv_task_SUS-RunIIWinter15wmLHE-00096__v1_T_151008_163320_7178').getSiteWhiteList()


#print findLostBlocks(url, '/SMS-T1tttt_mGluino-1650to1700_mLSP-1to1400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15FSPremix-MCRUN2_74_V9-v1/AODSIM')

#print getDatasetEventsAndLumis('/WWJJToLNuLNu_EWK_13TeV-madgraph-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v2/GEN-SIM')
#print findCustodialCompletion(url, '/GluGluToRadionToHHTo2B2G_M-260_narrow_13TeV-madgraph/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')

#si = siteInfo()
#to_go=defaultdict(set)
#for output in session.query(Output).filter(Output.datasetname.endswith('MINIAODSIM')).all():
#    if 'RunIISpring15MiniAODv2' in output.datasetname:
#        to_go[si.pick_SE()].add( output.datasetname )

#print len(to_go)
#for site in to_go:
#    print len(to_go[site]),"to",site
#    result = makeReplicaRequest(url, site, list(to_go[site]),"custodial copy of RunIISpring15MiniAODv2", custodial='y', priority='low', approve = (site in si.sites_auto_approve) )
#    print result

"""
for ds in filter(None, os.popen("grep 'because it is not custodial' ../logs/lockor/last.log | awk '{ print $3}'").read().split('\n')):
    items = getSubscriptions(url, ds)
    #print items
    for item in items['dataset']:
        for sub in item['subscription']:
            if 'MSS' in sub['node'] and sub['custodial']=='n':
                print ds,sub['node'],sub['custodial'],sub['request']
"""



#for wfo in session.query(Workflow).filter( Workflow.status == 'staging').all():
#    wfi = workflowInfo(url, wfo.name)
#    if wfi.request['RequestDate'][2] < 23-7:
#        print wfo.name, wfi.request['RequestDate']
#        wfo.status = 'considered'
#session.commit()


#print findLostBlocks(url, '/GJet_Pt-15To6000_TuneCUETP8M1-Flat_13TeV_pythia8/RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v3/AODSIM')

sys.exit(5)

#print dataCache.get('T1_DE_KIT_MSS_usage')

sys.exit(5)


#statuses = checkTransferLag( url, 515880, dataset="" )
#statuses = checkTransferLag( url, 515883 )
#statuses = checkTransferLag( url, 491310 )
#statuses = checkTransferLag( url, 503279 )
#statuses = checkTransferLag( url, 517998 ,datasets=['/QCD_Pt-15to7000_TuneCUETP8M1_Flat_13TeV_pythia8/RunIISpring15DR74-Asympt25nsRaw_MCRUN2_74_V9-v3/AODSIM'])
#statuses = checkTransferLag( url, 483745, datasets=['/TstarTstarToTgammaTgluon_M-1400_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v4/AODSIM'])
if False:
    missing_in_action=defaultdict(list)
    for line in filter(None, os.popen('grep incomple ../logs/stagor/last.log').read().split('\n')):
        (_,dataset) = line.split()
        #print dataset
        for sline in filter(None, os.popen('grep -A 1 "incomplete %s" ../logs/stagor/last.log | grep "{"'%dataset).read().split('\n')):
            tests=sline.replace("{","").replace("}","").split(',')
            for test in tests:
                test = test.strip()
                #print test
                (pid,check) = test.split(":")
                if "False" in check:
                    missing_in_action[dataset].append( int(pid) )
        

    open('incomplete_transfers.json','w').write(json.dumps( missing_in_action, indent=2))
    fdagfsd
    report = ""
    for phid in missing_in_action:
        print "test",phid
        issues = checkTransferLag( url, phid )
        for dataset in issues:
            for block in issues[dataset]:
                for destination in issues[dataset][block]:
                    (block_size,destination_size,rate,dones) = issues[dataset][block][destination]
                    report += "%s is not getting to %s, out of %s faster than %f [GB/s]\n"%(block,destination,", ".join(dones), rate)
                    print "%s is not getting to %s, out of %s faster than %f [GB/s]\n"%(block,destination,", ".join(dones), rate)

#print json.dumps( statuses, indent=2 )
#print getDatasetBlocks('/HLTPhysicspart4/Run2015C-v1/RAW', runs=[254790])

sys.exit(34)
#for wfl in getWorkflowByMCPileup(url, "/Neutrino_E-10_gun/RunIISpring15PrePremix-MCRUN2_74_V9-v1/GEN-SIM-DIGI-RAW", details=True):
#    if wfl['RequestStatus'] in ['acquired','assignment-approved','running-open','running-closed']:
#        print wfl['RequestName'], wfl['RequestStatus']

#wfn = 'jen_a_EXO-RunIISpring15MiniAODv2-00351_00059_v0__151015_215025_3022'
#wfn= 'pdmvserv_HIG-RunIISpring15MiniAODv2-00332_00043_v0__151008_035453_4934'

#for wfo in session.query(Workflow).filter(Workflow.name == wfn).all():
#    print wfo.name,wfo.status
#statuses=set()
#for wfo in session.query(Workflow).filter(Workflow.status.endswith('-unlock')).all():
#    statuses.add( wfo.status )
    #print wfo.name,wfo.status
    #wfo.status ='trouble'
#session.commit()
#print list( statuses ) 

#for wfo in session.query(Workflow).filter(Workflow.status == 'away-unlock').all():
#    print wfo.name,wfo.status
#    wfo.status ='away'
#session.commit()




sys.exit(1)
si = siteInfo()

t1=0
t2g=0
t2=0
for (site,cpu) in sorted(si.cpu_pledges.items(), key=lambda d : d[1], reverse=True):
    g=""
    if site in si.sites_with_goodIO:
        t2g+=cpu
        g="i/o"
    if site.startswith('T1'):
        t1+=cpu
    else:
        t2+=cpu
    print site,cpu,g

print t1
print t2g
print t2

sys.exit(2)


datasets=[]
sites=[ si.CE_to_SE(ce) for ce in si.sites_ready]
random.shuffle( sites )
for ce in sites[:4]:
    site = si.CE_to_SE(ce)
    print ce
    datasets.extend(filter(lambda w: w.count('/')==3, [w for w in itertools.chain.from_iterable([line.split() for line in os.popen('curl -s http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/result/%s/DeleteDatasets.txt'%site).read().split('\n')])]))

print len(datasets)
if True:
    random.shuffle(datasets)
    for dataset in datasets[:50]:
        if 'BACKFILL' in dataset: continue
        outs = getWorkflowByOutput(url, dataset,details=True)
        ins = getWorkflowByInput(url, dataset,details=True)
        pus = getWorkflowByMCPileup(url, dataset,details=True)
        statuses = list(set([r['RequestStatus'] for r in outs+ins+pus]))
        if any([s in ['assignment-approved','assigned','failed','acquired','running-open','running-closed','force-complete','completed','closed-out'] for s in statuses]):
            print dataset,"SHOULD NOT BE THERE",statuses
            continue
        status = getDatasetStatus( dataset )
        (_,_,_,tier) = dataset.split('/')
        if status == 'VALID' and not tier in ['DQMIO','DQM','MINIAODSIM']:
            custodials = findCustodialLocation(url, dataset)
            if len(custodials) == 0:
                print dataset,"SHOULD NOT BE THERE, NO CUSTODIAL"
                send
        print dataset,"rightfully deletable"


#print json.dumps( si.sites_pressure, indent=2)
#print si.sites_ready
#print 'T2_UA_KIPT' in si.sites_ready
#d= dataCache.get('gwmsmon_site_summary')

#for site in d:
#    print site,d[site]['MaxWasRunning']

#for wfo in session.query(Workflow).filter(Workflow.status == 'staging'):
#    wfi = workflowInfo( url , wfo.name)
#    (_,prim,_,_) = wfi.getIO()
#    for p in prim:
#        lost = findLostBlocks(url, p)
#        if len

#for d in json.loads( open('lost_blocks_datasets.json').read()):
    #lost = findLostBlocks(url, d)
    #print d
    #print len(lost)
#    usings = getWorkflowByInput(url, d, details=True)
#    print d,[u['RequestStatus'] for u in usings]
#    fract = getDatasetBlocksFraction(url, d)
#    print json.dumps( fract, indent=2)

#print findLostBlocks(url, sys.argv[1] )

#print d
#print 'T2_UA_KIPT' in d
#l = json.loads(open('datalocks.json').read())
#for s,info in l.items():
#    try:
#        if '/BlackHole_BH1_MD-5000_MBH-11000_n-2_TuneCUETP8M1_13TeV-blackmax/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/AODSIM' in #info:
#            print s
#    except:
#        pass

#wfs = getWorkflowByMCPileup(url, '/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM')
#wfs = getWorkflowByMCPileup(url, '/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',details=True)
#print set([r['RequestStatus'] for r in wfs])


#wfi = workflowInfo( url, 'pdmvserv_EXO-RunIISpring15DR74-02849_00421_v0__151004_184620_6176' ,stats=True)

#print json.dumps( wfi.wmstats, indent=2)
#from utils import dataCache

#print len(dataCache.get('gwmsmon_site_summary').keys())

sys.exit(23)

counts = defaultdict(int)
wfl=[
"pdmvserv_B2G-RunIISpring15MiniAODv2-00384_00018_v0__151007_104629_2988",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00357_00044_v0__151008_094331_7272",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00249_00032_v0__151007_175050_6219",
"pdmvserv_SUS-RunIISpring15MiniAODv2-00032_00037_v0__151007_181719_7743",
"pdmvserv_EXO-RunIISpring15MiniAODv2-01266_00098_v0__151012_130403_5373",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00019_00006_v0__151007_091225_8953",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00104_00009_v0__151007_101023_3657",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00120_00009_v0__151007_101218_4802",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00195_00011_v0__151007_102448_4239",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00207_00013_v0__151007_102634_7599",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00298_00015_v0__151007_103716_8300",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00324_00016_v0__151007_104117_4852",

"pdmvserv_HIG-RunIISpring15MiniAODv2-00024_00021_v0__151007_115424_687",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00204_00031_v0__151007_174557_5141",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00205_00031_v0__151007_174602_6898",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00205_00031_v0__151007_174602_6898",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00019_00006_v0__151007_091225_8953",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00019_00006_v0__151007_091225_8953",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00040_00007_v0__151007_100358_5724",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00104_00009_v0__151007_101023_3657",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00120_00009_v0__151007_101218_4802",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00127_00009_v0__151007_101753_1048",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00195_00011_v0__151007_102448_4239",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00207_00013_v0__151007_102634_7599",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00265_00014_v0__151007_103407_6419",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00298_00015_v0__151007_103716_8300",
"pdmvserv_B2G-RunIISpring15MiniAODv2-00324_00016_v0__151007_104117_4852",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00024_00021_v0__151007_115424_687",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00204_00031_v0__151007_174557_5141",
"pdmvserv_HIG-RunIISpring15MiniAODv2-00205_00031_v0__151007_174602_6898",
]
wfl=set(wfl)

for wf in wfl:
    wfi = workflowInfo( url, wf)
    wl = wfi.request['SiteWhitelist']
    print wl
    for s in wl:
        counts[s] += 1

#for wfo in session.query(Workflow).filter(Workflow.status=='assistance-manual').all():
#    if not 'MiniAODv2' in wfo.name: continue
#    if not wfo.name in wfl: continue
#    print wfo.name
#    wfi = workflowInfo( url, wfo.name)
#    wl = wfi.request['SiteWhitelist']
#    print wl
#    for s in wl:
#        counts[s] += 1

#si = siteInfo()
#for s in counts:
#    counts[s] = counts[s] / float(si.cpu_pledges[s])*100.

print "\n".join(["%s : %4.2f"%(item[0],item[1]) for item in sorted(counts.items(), reverse=True, key=lambda i :i[1])])



#presence = getDatasetPresence( 'cmsweb.cern.ch', '/Neutrino_E-10_gun/RunIISpring15PrePremix-MCRUN2_74_V9-v1/GEN-SIM-DIGI-RAW')
#print presence
#sec_location = [site for site,pres in presence.items() if pres[1]>90.]
#print sec_location
#subscriptions = listSubscriptions( url , '/Neutrino_E-10_gun/RunIISpring15PrePremix-MCRUN2_74_V9-v1/GEN-SIM-DIGI-RAW')
#sec_destination = [site for site in subscriptions]
#print subscriptions
#print sec_destination

#si=siteInfo()
#print 'T2_UA_KIPT' in si.sites_ready
#print 'T2_UA_KIPT' in si.sites_not_ready

#li= lockInfo()
#li.release('/QCD_Pt-40toInf_DoubleEMEnriched_MGG-80toInf_TuneCUETP8M1_13TeV_Pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM','T2_US_MIT',reason='unlocking')

#print getDatasetEventsPerLumi('/BlackHole_BH1_MD-4000_MBH-11000_n-6_TuneCUETP8M1_13TeV-blackmax/RunIIWinter15pLHE-MCRUN2_71_V1-v2/LHE')
#print getDatasetEventsPerLumi('/BlackHole_BH1_MD-4000_MBH-11000_n-6_TuneCUETP8M1_13TeV-blackmax/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')
#print getDatasetEventsPerLumi('/BlackHole_BH1_MD-4000_MBH-11000_n-6_TuneCUETP8M1_13TeV-blackmax/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/AODSIM')

#print len(session.query(Workflow).filter(Workflow.status.endswith('done')).all())
#for wfo in session.query(Workflow).filter(Workflow.status.endswith('done')).all():
#    wfo.status += "-unlock"
#session.commit()

#print getDatasetEventsPerLumi('/DYJetsToQQ_HT180_13TeV-madgraphMLM-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v3/GEN-SIM')

#print getDatasetEventsPerLumi('/DYJetsToQQ_HT180_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/AODSIM')

#def doThis():
#    htmlor()

#doThis()

#for wfo in session.query(Workflow).filter(Workflow.status == 'staged').all():
#    wfo.status = 'considered'
#session.commit()


#for site in si.all_sites:
#    pass
    #li.release('/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIMM', si.CE_to_SE(site), reason='typo')
#    li.lock('/Neutrino_E-10_gun/RunIISpring15PrePremix-MCRUN2_74_V9-v1/GEN-SIM-DIGI-RAW', si.CE_to_SE(site), reason='lock by hand')

#check_ggus( 112366 )




#for wfo in session.query(Workflow).filter(Workflow.name == 'jbadillo_B2G-RunIISpring15MiniAODv2-00254_00014_v0__151012_134742_2527').all():
#    print wfo.name,wfo.status

#wfs = session.query(Workflow).filter(Workflow.status=='away').all()
#random.shuffle( wfs )
#i=0
#for wfo in wfs:
#    print i
#    i+=1
#    wfi = workflowInfo( url , wfo.name )
#    if wfi.request['RequestStatus'] == 'closed-out':
#        print "closing",wfo.name
#        wfo.status = 'close'
#session.commit()

#print json.dumps( dict(getDatasetDestinations(url, '/BBbarDMJets_pseudoscalar_Mchi-150_Mphi-10000_13TeV-madgraph/RunIIWinter15wmLHE-MCRUN2_71_V1-v2/LHE',only_blocks=['/BBbarDMJets_pseudoscalar_Mchi-150_Mphi-10000_13TeV-madgraph/RunIIWinter15wmLHE-MCRUN2_71_V1-v2/LHE#ce8fb2a8-3525-11e5-90cd-a0369f23d008'])[0]), indent=2)
#print json.dumps( dict(getDatasetDestinations(url, '/BBbarDMJets_pseudoscalar_Mchi-150_Mphi-10000_13TeV-madgraph/RunIIWinter15wmLHE-MCRUN2_71_V1-v2/LHE')), indent=2)
#print json.dumps( dict(getDatasetDestinations(url, '/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM')))

#print sorted(getSiteWhiteList((False,'/DM_PseudoscalarWH_Mphi-1000_Mchi-10_gSM-1p0_gDM-1p0_13TeV-JHUGen/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',None,'/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM')))

#sys.exit(34)

#print getDatasetBlocksFraction( url, '/BBbarDMJets_pseudoscalar_Mchi-150_Mphi-10000_13TeV-madgraph/RunIIWinter15wmLHE-MCRUN2_71_V1-v2/LHE')

#wfh = workflowInfo(url ,'pdmvserv_SUS-chain_RunIIWinter15pLHE_flowRunIISpring15FSPreMix-00036__v1_T_150916_141334_4110')

#print wfh.getNCopies(CPUh=102683)

sys.exit(8)

"""
#print '\n'.join([wfo.name for wfo in session.query(Workflow).filter(Workflow.status.startswith('assistance')).filter(Workflow.name.startswith('pdmvserv_EXO-RunIIWinter15pLHE')).all()])

#print check_ggus( 115991 )


sys.exit(45)

wfh = workflowInfo(url ,'pdmvserv_TOP-RunIISummer15GS-00004_00042_v0__150905_121335_952')

wfh = workflowInfo(url ,'pdmvserv_TOP-RunIISummer15GS-00004_00042_v0__150905_121335_952')
print get_copies(wfh)
wfh = workflowInfo(url, 'pdmvserv_TOP-RunIISpring15DR74-00087_00352_v0__150909_092007_884')
print get_copies(wfh)

sys.exit(324)

"""
"""
for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
    wfi = workflowInfo(url, wfo.name )
    print wfi.getIO()

sys.exit(34)
"""


ds = '/BBbarDMJets_pseudoscalar_Mchi-1_Mphi-10000_13TeV-madgraph/RunIIWinter15wmLHE-MCRUN2_71_V1-v4/LHE'

destinations,all_block_names = getDatasetDestinations(url, ds, complement=False)

print json.dumps(destinations, indent=2)

destinations,all_block_names = getDatasetDestinations(url, ds, complement=True)

print json.dumps(destinations, indent=2)

sys.exit(23)
#acdc = 'vlimant_ACDC_EXO-RunIISpring15DR74-01648_00318_v0__150903_100554_7882'
#wfi = workflowInfo(url, acdc)
#print wfi.getSplittings()

factor = 2 
for split in wfi.getSplittings():
    for act in ['avg_events_per_job','lumis_per_job']: 
        if act in split:     
            print "Changing %s (%d) by a factor %d"%( act, split[act], factor),   
            split[act] /= factor    
            print "to",split[act] 
            break
    split['requestName'] = acdc
    print json.dumps( split, indent=2 )
    print reqMgrClient.setWorkflowSplitting(url, split )


#print [ task.taskType for task in  wfi.getAllTasks()]

sys.exit(4)
"""
si = siteInfo()

print len(si.all_sites)
print len(si.sites_ready)
print len([s for s,d in si.disk.items() if d])

sys.exit( 34)


print phedexClient.getCustodialSubscriptionRequestSite('/VBF_HToTauTau_M-125_14TeV-powheg-pythia6/GEM2019Upg14-DES19_62_V8-v1/GEN-SIM')

sys.exit(24)





"""

wfi = workflowInfo(url ,'pdmvserv_BPH-Summer12DR53X-00188_00414_v0__150820_193454_4274')
params = wfi.getSplittings()
print json.dumps(params, indent=2)
pprint.pprint( wfi.full_spec )
sys.exit(3)


ci = closeoutInfo()
ci.assistance()
sys.exit(545)
si = siteInfo()
si.fetch_glidein_info()
a = si.sitesByMemory( 3500 )
print a

sys.exit(3)
wfi = workflowInfo(url,'pdmvserv_SUS-RunIISpring15DR74-00051_00284_v0__150803_001642_3546')
wfi.getSummary()
for task,errors in wfi.summary['errors'].items():
    print task
    for name,codes in errors.items():
        if type(codes)==int: continue
        for errorCode,info in codes.items():
            print "Task",task,"had",info['jobs'],"failures with error code",errorCode,"in stage",name
    
    #print json.dumps( wfi.summary['errors'][task], indent=2 ) 

#LI = lockInfo()
#LI.clean_unlock(view=True)

sys.exit(1)

#si.fetch_detox_info()
#print json.dumps(si.disk, indent=2)

#ws = si.disk.values()


"""
while True:
    picked=None
    rnd = random.random() * sum(ws)
    for i, w in enumerate(ws):
        rnd -= w 
        if rnd < 0:
            print "pick:",i,"from",ws,"and",rnd
            picked=i
            break
    if picked==None:
        print "could not pick"
        break
"""


#wfi = workflowInfo(url, 'pdmvserv_task_TOP-RunIIWinter15wmLHE-00028__v1_T_150608_090934_3263')
#wfi.getSummary()
#print json.dumps( wfi.summary, indent=2 )

sys.exit(1)

"""
li = lockInfo()

li.release_everywhere('/GJet_Pt-20to40_DoubleEMEnriched_MGG-80toInf_TuneCUETP8M1_13TeV_Pythia8/RunIISpring15DR74-Asympt50ns_MCRUN2_74_V9A-v1/AODSIM',reason='hand cleaning test')
"""

"""
for wfo in session.query(Workflow).filter(Workflow.status=='away').filter(Workflow.name.contains('RunIIWinter15wmLHE')).all():
    print wfo.name
    wfi = workflowInfo(url, wfo.name)
    for out in [o for o in wfi.request['OutputDatasets'] if o.endswith('LHE')]:
        print "\t",out
        presence = getDatasetPresence(url, out)
        print presence
"""
#sys.exit(10)

"""
li = lockInfo()

for wfn in filter(None,os.popen("grep 'has finished' ../logs/cleanor/2015-05-{19,21,23}_22*.log | awk '{print $2}' | sort -u").read().split('\n')):
    print wfn
    wfo = session.query(Workflow).filter(Workflow.name == wfn).first()
    if wfo.status in ['clean','clean-out']:
        print "would need to send",wfo.name," back to done",wfo.status
        wfo.status = 'done'
#session.commit()
"""

"""
for wfo in session.query(Workflow).filter(Workflow.status=='away').all():
    wfi = workflowInfo(url, wfo.name)
    # lock the input at site whitelist
    (_,primary,_,_) = wfi.getIO()
    for prim in primary:
        for site in wfi.request['SiteWhitelist']:
            print "locking input",prim,"at",site
            li.lock(prim, site, 'catching up locks of prestaging')
    for out in wfi.request['OutputDatasets']:
        if 'FAKE' in out: continue
        for site in wfi.request['SiteWhitelist']:
            print "locking output",out,"at",site
            li.lock(out, site, 'catching up locks of prestaging')
"""

"""
bads = set()
for wfo in session.query(Workflow).filter(Workflow.status == 'away').all():
    wfi = workflowInfo(url, wfo.name)
    #if wfi.request['RequestStatus'] == 'acquired':
    if wfi.request['RequestStatus'] in ['running-closed','running-open','completed']:
        ## check 
        wfi.getWorkQueue()
        #print json.dumps( wfi.workqueue, indent=2)
        for work in wfi.workqueue:
            for o,l in work['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['Inputs'].items():
                if len(l)==1 and l[0]=='T0_CH_CERN' and not wfo.name in bads:
                    bads.add( wfo.name )
                    print wfo.name,work['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['Inputs']
            for o,l in work['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['PileupData'].items():
                if len(l)==1 and l[0]=='T0_CH_CERN' and not wfo.name in bads:
                    bads.add( wfo.name )
                    print wfo.name,work['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement']['PileupData']

"""

"""
ds = '/QCD_Pt-20to30_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM'

feeding = ['pdmvserv_BTV-RunIISpring15DR74-00001_00003_v0__150511_170003_5002','pdmvserv_BTV-RunIISpring15DR74-00067_00057_v0__150514_005833_9219','vlimant_BTV-RunIISpring15DR74-00029_00002_v0__150517_180140_5397']
ins = session.query(Workflow).filter(Workflow.name =='').first()
for tid in [443786]:
    tfo = session.query(Transfer).filter(Transfer.phedexid == tid).first()
    print tfo.id,tfo.phedexid
    for wf in tfo.workflows_id:
        wfo = session.query(Workflow).get( wf )
        print wfo.name,wfo.status
    for ins in [session.query(Workflow).filter(Workflow.name ==wfn).first() for wfn in feeding]:
        if not ins.id in tfo.workflows_id:
            l = copy.deepcopy( tfo.workflows_id )
            l.append( ins.id )
            print "adding",ins.name,"to",tid
            tfo.workflows_id = l 
            #session.commit()
    
"""
#SI = siteInfo()

#print json.dumps( SI.storage, indent=2 )

#sites_allowed = ['T1_DE_KIT', 'T1_IT_CNAF', 'T2_ES_CIEMAT', 'T2_IT_Rome', 'T2_DE_DESY', 'T2_US_Wisconsin', 'T1_ES_PIC', 'T2_US_MIT', 'T2_IT_Legnaro', 'T2_US_Caltech', 'T2_UK_London_Brunel', 'T2_US_Purdue', 'T2_UK_London_IC', 'T2_US_Nebraska', 'T2_IT_Pisa']
#t1_only = [ce for ce in sites_allowed if ce.startswith('T1')]

"""
trials=0
picked =set()
counts = defaultdict(int)
while True:
    trials+=1
    counts[ SI.pick_SE()] +=1
    continue
    one_pick = SI.pick_dSE([SI.CE_to_SE(ce) for ce in t1_only])
    if one_pick == 'T0_CH_CERN':
        print "got one",trials
        break
    picked.add( one_pick )
    one_pick =  SI.pick_dSE([SI.CE_to_SE(ce) for ce in sites_allowed])
    if one_pick == 'T0_CH_CERN':
        print "got one",trials,"on fallback and old setup"
        break
    picked.add( one_pick )
    if trials > 1000000:
        print trials
        break

print json.dumps( dict( counts) , indent=2)
"""
"""
dss = ['/QCD_Pt-15TTo7000_TuneZ2star-Flat_13TeV_pythia6/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
     '/QCD_Pt-20to30_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
     '/ST_t-channel_4f_leptonDecays_13TeV-amcatnlo-pythia8_TuneCUETP8M1/RunIIWinter15GS-MCRUN2_71_V1-v2/GEN-SIM',
     '/ST_tW_antitop_5f_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
     '/ST_tW_antitop_5f_scaleup_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
     '/ST_tW_top_5f_scaledown_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
     '/ST_tW_top_5f_scaleup_inclusiveDecays_13TeV-powheg-pythia8_TuneCUETP8M1/RunIIWinter15GS-MCRUN2_71_V1-v2/GEN-SIM',
     '/TTJets_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
     '/TTJets_TuneCUETP8M1_13TeV-amcatnloFXFX-scaleup-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
     '/TT_TuneCUETP8M1_13TeV-powheg-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM',
       ]

wfs = []
for ds in dss:
    members = getWorkflowByInput(url, ds)
    for member in members:
        wfo = session.query(Workflow).filter( Workflow.name == member).first()
        if wfo:
            if wfo.status in ['away','considered','staging']:
                wfs.append( wfo.id )
                print "\t\t",wfo.status,wfo.name
            else:
                print wfo.status,wfo.name
"""

"""
for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
    wfi = workflowInfo(url, wfo.name)
    dataset = wfi.request['InputDataset']
    available = getDatasetBlocksFraction( url , dataset )
    if available>2.:
        print "\t\t",wfo.name,"can go staged"
"""
    
#tfo = Transfer( phedexid = 443786, workflows_id = wfs)
#session.add( tfo )
#session.commit()

#tfo = session.query(Transfer).filter(Transfer.phedexid == 443786).first()
#print tfo 

#for wfid in tfo.workflows_id:
#    wfo = session.query(Workflow).get( wfid)
#    print wfo.name,wfo.status

#sys.exit( 12) 
"""
for wfo in session.query(Workflow).filter(Workflow.status.endswith('-out')).all():
    print wfo.status
    wfo.status = wfo.status.replace('-out','')
    print wfo.status
    session.commit()
"""
"""
#wf = session.query(Workflow).filter(Workflow.name == 'pdmvserv_HIG-RunIIWinter15GenOnly-00030_00003_v0__150511_093921_1126').first()
wf = session.query(Workflow).filter(Workflow.name == 'pdmvserv_HIG-RunIIWinter15GenOnly-00029_00003_v0__150511_093922_2258').first()
for ti in [443244, 443245, 443246, 443247, 443248, 443249, 443250, 443251, 443252, 443253, 443254, 443255, 443256, 443257, 443258, 443259, 443260, 443261]:
    tf = session.query(Transfer).filter(Transfer.phedexid == ti).first()
    print tf.workflows_id
    if not wf.id in tf.workflows_id:
        print "missing"
        l = copy.deepcopy( tf.workflows_id )
        l.append( wf.id )
        tf.workflows_id = copy.deepcopy(l)
        print tf.workflows_id
        session.commit()
"""

"""
print updateSubscription(url, 
                         'T2_US_Nebraska',
                         '/ReggeGribovPartonMC_castorJet_13TeV-QGSJetII/RunIIWinter15GS-castor_MCRUN2_71_V0-v2/GEN-SIM#7b2047a6-e203-11e4-bead-a0369f23d138',
                         priority = 'high'
                         )

"""

"""
sites_and_datasets=defaultdict(list)
for wfo in session.query(Workflow).filter(Workflow.status == 'clean').all():
    wfi = workflowInfo(url, wfo.name)
    if 'InputDataset' in wfi.request:
        out = wfi.request['InputDataset']
        presence_none = getDatasetPresence(url, out, group = "")
        presence_dataops = getDatasetPresence(url, out, group = "DataOps")
        if len(presence_dataops):
            print "no no",out
            print json.dumps( presence_dataops, indent=2)

        for site in presence_none:
            sites_and_datasets[site].append( out )
        print out
        print "left overs at",len(presence_none),"sites"

print json.dumps( sites_and_datasets, indent = 2)
open('leftovers.json','w').write( json.dumps( sites_and_datasets, indent = 2) )
"""

        
"""
wfi = workflowInfo(url, 'pdmvserv_HIG-TP2023SHCALDR-00027_00008_v0__150329_182803_7806')
ns = wfi.getSchema()
print json.dumps( ns, indent=2)
print ns['ProcessingString']
"""


"""
wfs = ['alahiff_EGM-Fall14DR73-00008_00022_v0__150329_110300_5744']
for wf in wfs:
    if session.query(Workflow).filter(Workflow.name==wf).first(): 
        print "already there"
        continue
    nwf = Workflow(name = wf )
    session.add( nwf)
    session.commit()   
"""

"""
for wfo in session.query(Workflow).filter(Workflow.status == 'staging').all():
    helper=[]
    for tro in session.query(Transfer).all():
        if wfo.id in tro.workflows_id:
            print wfo.name,"has",tro.phedexid
            helper.append( tro.phedexid )
    if not helper:
        print wfo.name,"with no transfer"
"""

#wfo = session.query(Workflow).filter(Workflow.name == 'cmsdataops_HCA-Fall14DR73-00001_00008_v0__150422_225101_4249').first()
#wfi = workflowInfo(url, wfo.name)
#print wfi.request['RequestStatus']
#session.delete( wfo )
#session.commit()
#print json.dumps(wfi.getSchema(), indent=2)

#sys.exit(11)

"""
out_by_wf ={}
for out in session.query(Output).all():
    if not out.workfow_id in out_by_wf: out_by_wf[out.workfow_id]=[]
    out_by_wf[out.workfow_id].append( out.datasetname )
for (wf,outs) in out_by_wf.items():
    vs = set([int(o.split('/')[-2].split('-')[-1].replace('v',''))for o in outs])
    if len(vs)==1:
        ##more than one version
        out_by_wf.pop(wf)
#print json.dumps(out_by_wf,indent=2)
"""

"""
for (wf,outs) in out_by_wf.items():
    wfo = session.query(Workflow).get(wf)
    for out in outs:
        makings = getWorkflowByOutput(url, out, details=True)
        for making in makings:
            if making['RequestType']=='Resubmission': continue
            wfoo = session.query(Workflow).filter(Workflow.name == making['RequestName']).first()
            if wfoo:
                outo = session.query(Output).filter(Output.datasetname == out).first()
                if outo:
                    outo.workfow_id = wfoo.id
                    print wfoo.id,"instead of",wfo.id,"for",out
                    session.commit()
"""
#schema = retrieveSchema("")
"""
tr = session.query(Transfer).filter(Transfer.phedexid==440046).first()
print tr.workflows_id
wf = session.query(Workflow).get(tr.workflows_id[0])
print wf.name
owf = session.query(Workflow).filter(Workflow.name == 'pdmvserv_TOP-Summer12DR53X-00301_00378_v0__150330_215858_9864').first()
nwf = session.query(Workflow).filter(Workflow.name == 'vlimant_TOP-Summer12DR53X-00301_00378_v0__150421_151236_6237').first()
#wf = session.query(Workflow).get(tr.workflows_id[0])
#print wf.name
ids = copy.deepcopy(tr.workflows_id)
ids.remove(owf.id)
ids.append(nwf.id)
print ids
tr.workflows_id = ids
#session.commit()
"""

"""
delete_per_site = json.loads(open('deletes.json').read())
sites = set()
datasets = set()
#result = makeDeleteRequest(url,','.join( sites ), datasets, comments="cleanup after production")

for site in delete_per_site:  
    list_dataset = [info[0] for info in delete_per_site[site]]
    #result = makeDeleteRequest(url,site, list_dataset, comments="cleanup after production")
    #print result
    datasets.update([info[0] for info in delete_per_site[site]])
    sites.add(site)


print sites
print datasets
####print makeDeleteRequest(url ,sites ,datasets, comments="cleanup after production") 


sys.exit(10)
"""

#for wfo in session.query(Workflow).filter(Workflow.status == 'clean').all():
#    wfi = workflowInfo( url, wfo.name)
#    print wfi.request['InputDataset']
    
    

#wf = session.query(Workflow).filter(Workflow.name=='dmason_EXO-RunIIWinter15GS-00734_00052_v0__150417_231132_8083').first()
#print wf.name
#session.delete( wf )
#session.commit()
#sys.exit(1)

#wfo = session.query(Workflow).filter(Workflow.name == 'pdmvserv_HIG-TP2023SHCALDR-00020_00008_v0__150329_182738_4161').first()
#print wfo.status
#wfo.status = 'nono'
#session.commit()
#wfo.status = 'forget'
#session.commit()

"""
ds_to_sites = json.loads(open('mss.list').read())
for site in ds_to_sites:
    if 'CERN' in site:
        print site,"is fine"
        continue
    print site
    print json.dumps(ds_to_sites[site],indent=2)
"""

"""
ds_to_sites = {}
for wfo in session.query(Workflow).filter(Workflow.status=='away').all()+session.query(Workflow).filter(Workflow.status=='done').all():
    wfi = workflowInfo(url, wfo.name, deprecated=True)
    if not 'SubscriptionInformation' in wfi.deprecated_request: 
        print wfo.name,"no info"
        continue
    
    for (ds,sub) in wfi.deprecated_request['SubscriptionInformation'].items():
        print wfo.name,sub['AutoApproveSites']
        mss = filter(lambda s : 'MSS' in s, sub['AutoApproveSites'])
        if len(mss):
            print "\t",wfo.name,"has",sub['AutoApproveSites']
            print "\t",wfi.request['OutputDatasets']
            for s in mss:
                if not s in ds_to_sites: ds_to_sites[s]=set()
                ds_to_sites[s].update(wfi.request['OutputDatasets'])
            break

for s in ds_to_sites:
    ds_to_sites[s] = list(ds_to_sites[s])
    ds_to_sites[s].sort()
    print ds_to_sites[s]
open('mss.list','w').write(json.dumps(ds_to_sites, indent=2))
"""

#sys.exit(10)    
#out = session.query(Output).filter(Output.datasetname == '/SingleNeutrino/Fall14DR73-Ave40bx25_tsg_MCRUN2_73_V11-v1/DQMIO').first()
#print out.workfow_id
#wf = session.query(Workflow).get(out.workfow_id)
#print wf
#session.delete(out)
#session.commit()

#tr = session.query(Transfer).filter(Transfer.phedexid==441155).first()
#for wfid in tr.workflows_id:
#    wfo = session.query(Workflow).get(wfid)
#    print wfo.name

#wf = session.query(Workflow).filter(Workflow.name=='jen_a_HCA-Fall14DR73-00002_00009_v0__150327_210008_3674').first()
#wf = session.query(Workflow).filter(Workflow.name=='jen_a_HIG-TP2023HGCALDR-00014_00003_v0__150412_180828_4973').first()
#session.delete(wf)
#print wf
#session.commit()

"""
sys.exit(23)

tr = session.query(Transfer).filter(Transfer.phedexid==440197).first()
for wfid in tr.workflows_id:
    wfo = session.query(Workflow).get(wfid)
    if wfo.status =='forget':
        print wfid,wfo.name,wfo.status
        pid = filter(lambda chunk : chunk.count('-')==2,wfo.name.split('_'))[0]
        familly = getWorkflowById( url, pid )
        for member in familly:
            new_wf = session.query(Workflow).filter(Workflow.name == member).first()
            if new_wf:
                sw = copy.deepcopy(tr.workflows_id)
                sw.remove( wfo.id)
                sw.append(new_wf.id)
                tr.workflows_id = sw
                print tr.phedexid,"got",new_wf.name
                session.commit()
                break

for tr in session.query(Transfer).all():
    for wfid in tr.workflows_id:
        wfo = session.query(Workflow).get(wfid)
        if wfo.status == 'considered':
            wfo.status = 'staging' 
            print "changing",wfo.name,"to staging in",tr.phedexid
            #session.commit()


sys.exit(1)
"""

#wfi = workflowInfo( url, "pdmvserv_HIG-2019GEMUpg14DR-00116_00086_v0__150330_112405_8526")
#wfi = workflowInfo( url, "pdmvserv_task_B2G-RunIIWinter15wmLHE-00001__v1_T_150402_161327_2265")
#wfi = workflowInfo( url, "pdmvserv_HIG-TP2023SHCALDR-00027_00008_v0__150329_182803_7806")
#wfi = workflowInfo( url, "jen_a_BTV-TP2023HGCALDR-00002_00002_v0__150407_161443_2246")
#wfi = workflowInfo( url, "jen_a_HIG-TP2023SHCALDR-00029_00008_v0__150407_161522_3546")
#checkWorkflowSplitting(url, 'jen_a_BTV-TP2023HGCALDR-00002_00002_v0__150407_161443_2246')
#sys.exit(1)

"""
#print wfi.full_spec.__dict__.keys()
#print wfi.full_spec.tasks.Production.input.splitting.algorithm
splits = wfi.getSplittings()
print splits[0]
#print wfi.request.keys()
evt_per_lumi_in = wfi.request['TotalInputEvents'] / float(wfi.request['TotalInputLumis'])
time_per_one_lumi_in = evt_per_lumi_in * wfi.request['TimePerEvent']
if splits[0]['events_per_job']< evt_per_lumi_in:
    print "this is not going to work",evt_per_lumi_in,"in input and need to run",splits[0]['events_per_job'],"per job"
#print wfi.full_spec.tasks.__dict__
#print getattr(wfi.full_spec.tasks, 'B2G-RunIIWinter15wmLHE-00001_0').input.splitting.__dict__.keys() #['B2G-RunIIWinter15wmLHE-00001_0'].input.__dict__.keys()
"""
#print wfi.full_spec.tasks.StepOneProc.input.splitting.__dict__
#print wfi.full_spec.tasks.StepOneProc.tree.__dict__
#print wfi.getIO()

#print wfi.acquisitionEra()

#pid = sys.argv[1]
#tr = session.query(Transfer).filter(Transfer.phedexid== pid).first()
#for wfid in tr.workflows_id:
#    wf = session.query(Workflow).get(wfid)
#    print wf.id,wf.name
#    wf.status = 'staging'
#session.commit()

#checks = checkTransferStatus( 'cmsweb.cern.ch',440053 )
#print checks

#tr = session.query(Transfer).filter(Transfer.phedexid== 440100 ).first()
#session.delete( tr )
#session.commit()

#wf = session.query(Workflow).filter(Workflow.name=="pdmvserv_TOP-Summer12DR53X-00302_00379_v0__150331_100923_4420"
#                                    ).first()

#wl = getWorkLoad( 'cmsweb.cern.ch', wf.name )
#pprint.pprint( wl )
#wf.status = 'staging'
#session.commit()

#for out in session.query(Output).all():
#    if  out.workflow.status == 'done':
#        print "%150s on week %s"%(out.datasetname,time.strftime("%W (%x %X)",time.gmtime(out.date)))

#for out in session.query(Output).all():
#    print out.status

#print "\n\n"
#for out in session.query(Output).all():
#    if  out.workflow.status == 'away':
#        print "%150s %d/%d = %3.2f%% %s %s "%(out.datasetname,
#                                              out.nlumis,
#                                              out.expectedlumis,
#                                              out.nlumis/float(out.expectedlumis)*100.,
#                                              out.workflow.name,
#                                              out.workflow.status)
    
#print "\n\n"

#for wf in session.query(Workflow).filter(Workflow.name=='pdmvserv_TAU-2019GEMUpg14DR-00038_00084_v0__150329_162031_7993').all():
#

"""
for wf in session.query(Workflow).filter(Workflow.status == 'considered').all(): 
    print "workflow:",wf.name,wf.id,wf.status,wf.wm_status   

"""

"""
existing = [wf.name for wf in session.query(Workflow).all()]
take_away=[]
for each in existing:
    if existing.count(each)!=1:
        print each
        take_away.append(each)

for each in take_away:
    for (i,wf) in enumerate(session.query(Workflow).filter(Workflow.name==each).all()):
        if i:
            session.delete( wf )
            session.commit()
"""
