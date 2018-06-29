from assignSession import *
import time
from utils import getWorkLoad, checkTransferStatus, workflowInfo, getWorkflowById, makeDeleteRequest, getWorkflowByOutput, getDatasetPresence, updateSubscription, getWorkflowByInput, getDatasetBlocksFraction, siteInfo, getDatasetDestinations, check_ggus, getDatasetEventsPerLumi, getWorkflowByMCPileup, getDatasetStatus, getDatasetBlocks, checkTransferLag, listCustodial, listRequests, getSubscriptions, makeReplicaRequest,getDatasetEventsAndLumis, getLFNbase, getDatasetFiles, getDatasetBlockSize, getWorkflowById
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
import os

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

dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

#print len(session.query(Workflow).all())
#print len(session.query(Output).all())
#print len(session.query(Transfer).all())

## find out from the completed ACDC whether there is something more to do with it
completed = getWorkflows(url, 'completed', user='vlimant', rtype='Resubmission')
for wf in completed:
    #if 'recovery' in wf and '2016' in wf:
    #if all(key in wf for key in ['recovery','2016']):
    if all(key in wf for key in ['18Apr2017','recovery']):
        print wf,"is completed"
        makeall = os.popen('./makeACDC.py --all -w %s'% wf).read()
        if True or 'Tasks: []' in makeall:
            print "and happily finsihed with no ACDC, close the JIRA"
            reqMgrClient.closeOutWorkflow(url, wf)
            time.sleep(1)
            reqMgrClient.announceWorkflow(url, wf)
        else:
            print "There are some ACDC to be assigned"
            print makeall
    

affected_blocks =set()
needed_blocks = set()
needed_dataset = set()
original_wf = set()

affected_dataset = set()
affected_files = defaultdict(set)
#for fn in open('/afs/cern.ch/user/v/vlimant/public/ops/new2016.txt').read().split('\n'):
for fn in open('/afs/cern.ch/user/v/vlimant/public/ops/vandy_2017.txt').read().split('\n'):
    if not fn:
        continue
    block = dbs.listBlocks( logical_file_name = fn)
    for bi in block:
        bn = bi['block_name']
        affected_blocks.add( bn )
        affected_dataset.add( bn.split('#')[0] )
        affected_files[ bn.split('#')[0]].add( fn )
        ## find it's parent
        block_parents = dbs.listBlockParents( block_name = bn )
        for p_block  in block_parents:
            needed_blocks.add( p_block['parent_block_name'] )
        ds = bn.split('#')[0]
        wfs = getWorkflowByOutput( url, ds ,details=True)
        t_wfs = filter(lambda wf : not any(key in wf['RequestName'] for key in ['recovery','ACDC','RC','Recovery']), wfs)
        original_wf.update( map(lambda o:"%s %s"%(o['RequestName'],o['PrepID']), t_wfs) )
        needed_dataset.update( [wf['InputDataset'] for wf in wfs if 'InputDataset' in wf] )

print "Affected datasets"
print '\n'.join( sorted(affected_dataset ))

print "Affected blocks"
print '\n'.join( sorted(affected_blocks ))

print "Needed blocks of RAW data"
print '\n'.join( sorted(needed_blocks ))

print "Needed RAW datasets"
print '\n'.join( sorted(needed_dataset ))

print "Locations"
for dataset in needed_dataset:
    blocks = [b for b in needed_blocks if b.split('#')[0]==dataset]
    there = [ site for site,(t,f) in getDatasetPresence(url, dataset, only_blocks=blocks).items() if t]
    if not there:
        for b in blocks:
            there = [ site for site,(t,f) in getDatasetPresence(url, dataset, only_blocks=[b]).items() if t]
            print b,there
    else:
        print dataset,len(blocks),"blocks are at",there

print "workflows to recover from"
print '\n'.join( sorted(original_wf))


#affected_dataset = ['/JetHT/Run2016B-23Sep2016-v1/AOD']
#sevendaysago = 1479993065 #time.mktime(time.gmtime()) - (7*24*60*60)
sevendaysago = time.mktime(time.gmtime()) - (7*24*60*60)
sevendays = time.mktime(time.gmtime()) + (7*24*60*60)
recovered_files = defaultdict(set)
for dataset in sorted(affected_dataset):
    print "new files for",dataset
    for new_block in dbs.listBlocks( dataset=dataset, min_cdate = int(sevendaysago), max_cdate = int(sevendays)):
        print "in block",new_block['block_name']
        for ifile in dbs.listFiles(block_name = new_block['block_name'],detail=True):
            iname = ifile['logical_file_name']
            filedate = ifile['last_modification_date']
            print "\n",iname
            recovered_files[dataset].add( iname )


twiki_table="| *Affected Dataset* | *Corrupted Files* | *Recover Files (no 1-2-1 correspondence)* |\n"
full_picture = []
## make a nice summary page
for dataset in sorted(affected_dataset):
    twiki_table+='| %s |'%(dataset)
    twiki_table+='%s |'%( '<br>'.join( affected_files[dataset] ))
    twiki_table+='%s |\n'%( '<br>'.join( recovered_files[dataset] ))
    full_picture.append( { 'dataset' : dataset,
                           'affected' : sorted(affected_files[dataset]),
                           'recovered' : sorted(recovered_files[dataset])
                           }
                         )


open('%s/corrupted_twiki.txt'%monitor_dir,'w').write( twiki_table )
open('%s/corrupted.json'%monitor_dir,'w').write( json.dumps ( full_picture, indent=2) )

##create all json for making requests
#for wf in original_w: os.system('./recoverMissingLumis.py -q vlimant -g DATAOPS -r  %s'% wf)

gfaf


b,f = findLostBlocksFiles(url, '/QCD_Pt_600to800_TuneCUETP8M1_13TeV_pythia8/RunIISummer15GS-MCRUN2_71_V1_ext1-v2/GEN-SIM')

print b
print f

fdagfds

blob = json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/dataset_priorities.json').read())
presences = defaultdict( dict )
for prio in blob:
    for dataset in blob[prio]:
        presences[dataset] = getDatasetPresence( url, dataset ,vetoes=['Export','Buffer'])

#print json.dumps( presences, indent=2)
open('/afs/cern.ch/user/c/cmst2/www/unified/dataset_presence.json','w').write( json.dumps( presences, indent=2) )
fdafgsd

blob = json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/dataset_requirements.json').read())

by_priority = defaultdict(list)
for wf in blob:
    wfi = workflowInfo(url, wf)
    by_priority[wfi.request['RequestPriority']] = list(set(by_priority[wfi.request['RequestPriority']]+  blob[wf] ))

open('/afs/cern.ch/user/c/cmst2/www/unified/dataset_priorities.json','w').write( json.dumps( by_priority, indent=2) )

fafds

#print getDasetEventsAndLumis( '/MajoranaNeutrinoToMuE_M-500_TuneCUETP8M1_13TeV-alpgen_v2/RunIIWinter15pLHE-MCRUN2_71_V1-v1/LHE')

"""
blob = json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/incomplete_transfers.json').read())

before = time.mktime(time.gmtime()) - (30*24*60*60)
then = time.mktime(time.gmtime()) + (5*24*60*60)

conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

atjinr = filter(lambda i : i.startswith('/'), map(lambda l : l.split()[0], filter(None, open('transfers.txt').read().split('\n'))))
dontsuspend = filter(None, open('important.txt').read().split('\n'))

random.shuffle( atjinr ) 

for dataset in atjinr:
    for transferid in blob[dataset]:
        there = '/phedex/datasvc/json/prod/subscriptions?request=%d&create_since=%d&suspended=n'% (transferid, before)
        print dataset
        print there
        r1=conn.request("GET", there)
        r2=conn.getresponse()
        result = json.loads(r2.read())['phedex']
        for ds in result['dataset']:
            dsn = ds['name']

            if not dsn in atjinr: 
                print dsn,"not resident on JINR tape"
                continue

            if dsn in dontsuspend: 
                print dsn,"is IMPORTANT"
                continue

            print dsn,"via",transferid
            if 'subscription' in ds:
                for sub in ds['subscription']:
                    destination = sub['node']
                    print "\tsuspending",dsn,"at",destination
                    ##updateSubscription(url, destination, dsn, suspend= then)

            if 'block' in ds:
                for block in ds['block']:
                    blockn = block['name']
                    for sub in block['subscription']:
                        destination = sub['node']
                        print "\t\tsuspending",blockn,"at",destination
                        ##updateSubscription(url, destination, blockn, suspend= then)

fdafds
"""

"""
for dataset in blob.keys():
    presence = getDatasetPresence(url, dataset, within_sites=['T1_RU_JINR_MSS'], vetoes=[])
    if presence:
        print "\t",dataset,presence
    else:
        pass
        #print dataset

fadfa
"""


wfs = getWorkflows(url, status='assignment-approved', priority=90000, details=True)
all_in = set()
for wfr in wfs :
    wf = wfr['RequestName']
    if not 'DR80Premix' in wf: continue
    wfi = workflowInfo(url, wf, request = wfr)
    if wfi.request['RequestPriority'] >= 90000:
        if 'InputDataset' in wfi.request:
            all_in.add( wfi.request['InputDataset'])

print '\n'.join( all_in )

fdasfs
    
#wfs = getWorkflows(url, status='closed-out')
wfs = []
wfs.extend( getWorkflows(url, status='acquired') )
wfs.extend( getWorkflows(url, status = 'running-closed'))
wfs.extend( getWorkflows(url, status = 'running-open'))
for wf in wfs :
    if not 'DR80Premix' in wf: continue

    wfi = workflowInfo(url, wf)
    spec= wfi.get_spec()
    ncore = spec.tasks.StepOneProc.steps.cmsRun1.tree.children.cmsRun2.application.multicore.numberOfCores
    #crap = (ncore == 1)
    assigned_log = filter(lambda change : change["Status"] in ["assigned","acquired"],wfi.request['RequestTransition'])
    crap = (ncore==1) or (ncore==4 and assigned_log and assigned_log[0]['UpdateTime'] < 1479481842)
    prio = wfi.request['RequestPriority']

    asked = wfi.request['RequestNumEvents'] if 'RequestNumEvents' in wfi.request else None
    if crap:
        outs = filter(lambda ds: ds.endswith('/AODSIM'), wfi.request['OutputDatasets'])
        out = outs[0] if outs else None
        if out:
            ne,nl = getDatasetEventsAndLumis( out )
            
            completed = ne/float(asked) if asked else None
            if (completed!=None and completed < 0.1) or (ne < 10000):
                print out,ne,wf
            else:
                completed_s = "%.2f%%"%(100*completed) if completed!=None else "no fraction"
                print "\twould be loosing",ne,completed_s,wf,prio
faf

#wfi = workflowInfo(url,'pdmvserv_task_EXO-RunIIFall15DR76-04677__v1_T_161019_035339_1562', spec=False)
#print wfi.getSiteWhiteList()

#print reqMgrClient.changePriorityWorkflow(url, sys.argv[1], int(sys.argv[2]))

#gfsdgf

"""
agents = getAllAgents( url )
for team,agents in agents.items():
    for agent in agents:
        print agent['status'],agent['agent_url'].split(':')[0]
        if 'down_component_detail' in agent:
            print agent['down_component_detail']
        if 'down_components' in agent:
            print agent['down_components']
fdsfd
print getDatasetBlocks( '/DisplacedJet/Run2016B-v2/RAW' )
wfi = workflowInfo(url, 'cerminar_Run2016B-v2-DisplacedJet-23Sep2016_8020_161020_182850_609')
a,b,c,d = wfi.getRecoveryBlocks()
print len(a)
print len(b)
print b
gfdsgf
"""
dataset='/JetHT/Run2016B-23Sep2016-v2/DQMIO'

in_dbs,in_phedex,missing_phedex,missing_dbs  = getDatasetFiles( url, dataset, without_invalid=False)
#print len(in_dbs)
#print len(in_phedex)
print missing_phedex
print missing_dbs
#a,b = dbs3Client.duplicateRunLumiFiles( '/BTagMu/Run2016E-23Sep2016-v1/DQMIO', skipInvalid=True, verbose=True)
#print a
#print b

fagdf

"""
wfs = []
wfs.extend(getWorkflows(url, 'normal-archived', user='cerminar', rtype='ReReco'))
wfs.extend(getWorkflows(url, 'announced', user='cerminar', rtype='ReReco'))

known_and_OK = json.loads(open('known_and_OK.json').read())
r_done = ['cerminar_Run2016C-v2-JetHT-23Sep2016_8020_160923_181939_7733',
          'cerminar_Run2016B-v2-MuOnia-23Sep2016_8020_160923_164235_9184',
          'cerminar_Run2016F-v1-DoubleEG-23Sep2016_8020_160926_174056_8845',
          'cerminar_Run2016D-v2-MuOnia-23Sep2016_8020_160926_170117_6336',
          'cerminar_Run2016E-v2-JetHT-23Sep2016_8020_160926_171408_7283',
          'cerminar_Run2016B-v1-SingleMuon-23Sep2016_8020_160923_164429_8880',
          'cerminar_Run2016B-v2-Charmonium-23Sep2016_8020_160923_163401_8524'
          'cerminar_Run2016C-v2-DoubleEG-23Sep2016_8020_160923_181709_9821',
          'cerminar_Run2016C-v2-MuOnia-23Sep2016_8020_160923_182039_9567',
          'cerminar_Run2016D-v2-JetHT-23Sep2016_8020_160926_170014_145',
          'cerminar_Run2016E-v2-SingleMuon-23Sep2016_8020_160926_171635_1197',
          'cerminar_Run2016G-v1-JetHT-23Sep2016_8020_160926_173404_1936'
          ]

r_done = []
known_and_OK = []
for wfn in sorted(wfs):
    if not '23Sep2016' in wfn: continue
    if wfn in known_and_OK: 
        print wfn,"nothing to be done"
        continue
    if wfn in r_done:
        print "\t",wfn,"dealt with"
        continue
    wfi = workflowInfo(url, wfn)
    wf = wfi.request
    outs = wf['OutputDatasets']
    #skims = filter( lambda d : any([d.endswith('/'+tier) for tier in ['USER','RAW-RECO']]), outs)
    skims = filter( lambda d : any([d.endswith('/'+tier) for tier in ['ALCARECO']]), outs)
    tiers = [out.split('/')[-1] for out in outs]
    #print sorted(set(tiers))
    need_recover = False
    for skim in skims:
        oo = session.query(Output).filter(Output.datasetname == skim).first()
        if oo:
            if oo.expectedlumis != oo.nlumis:
                fraction_passed = float(oo.nlumis) / float(oo.expectedlumis)
                presence = getDatasetPresence(url, wf['InputDataset'])
                print "*"*20
                print wfn
                print skim,"needs recover",oo.expectedlumis,oo.nlumis,"%.2f%%"%(100.*fraction_passed)
                print json.dumps( presence, indent=2)
                need_recover = True
                command = 'python recover/recoverMissingLumis.py -q vlimant -g DATAOPS -r %s'% wfn
                print command
                print "*"*20
                #os.system( command )
            else:
                print skim,"is fine"
        else:
            print skim,"is not a known output"
    if need_recover:
        ## run the recovery script
        ## identify the needed blocks
        ## transfer to FNAL
        ## assign to FNAL
        pass
    else:
        known_and_OK.append( wfn )

#open('known_and_OK.json','w').write( json.dumps( known_and_OK , indent=2))

fdsgf
"""

#wfi = workflowInfo(url, 'cerminar_Run2016E-v2-Tau-23Sep2016_8020_160926_171733_2493')
wfi = workflowInfo(url, sys.argv[1] )

doc = wfi.getRecoveryDoc()
all_files = set()
for d in doc:
    #print d['files']
    all_files.update( d['files'].keys())

print len(all_files),"file in recovery"
dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
all_blocks = set()
files_no_block = set()
for f in all_files:
    #print f
    r = dbsapi.listFileArray( logical_file_name = f, detail=True)
    if not r:
        files_no_block.add( f) 
    else:
        all_blocks.update( [df['block_name'] for df in r ])
    
print '\n'.join(all_blocks)
fdasfd


##aggregate for everything running

total_by_task = defaultdict(lambda : defaultdict(int))
code_by_task = defaultdict(lambda : defaultdict(int))


for wfo in session.query(Workflow).filter(Workflow.status=='away').all()[:10]:
    wfi = workflowInfo(url, wfo.name)
    cache=100000 ## whatever there is
    err= wfi.getWMErrors(cache=cache)
    stat = wfi.getWMStats(cache=cache)
    #dashb = wfi.getFullPicture(since=30,cache=cache)   
    
    for agent in stat['AgentJobInfo']:
        for task in stat['AgentJobInfo'][agent]['tasks']:
            pass

#wfn = 'cerminar_Run2016B-v1-JetHT-23Sep2016_8020_160923_164022_7919'
#wfi = workflowInfo(url, wfn)       

#stats = wfi.getWMStats(cache=500)

fasfd

"""
wfs = []
#wfs.extend(getWorkflows(url, 'acquired', rtype='ReReco', details=True))
wfs.extend(getWorkflows(url, 'running-open', rtype='ReReco', details=True))
wfs.extend(getWorkflows(url, 'running-closed', rtype='ReReco', details=True))

for wf in wfs:
    wfn = wf['RequestName']
    wfi = workflowInfo(url, wfn, request=wf)
    dataset = wfi.request['InputDataset']
    presence = getDatasetPresence(url, dataset)
    in_full = [site for site,(_,there) in presence.items() if there]
    if not 'T2_CH_CERN' in in_full:
        #print dataset,wf['RequestStatus'],
        print wf['RequestName']
        #print in_full
    else:
        pass
"""

"""
wfs = getWorkflows(url, 'running-closed', details=True)
#wfs = getWorkflows(url, 'running-open', details=True)
for wf in wfs:
    wfn = wf['RequestName']
    wfi = workflowInfo(url, wfn, request=wf)
    if not 'HIG-RunIISummer15wmLHEGS' in wfn :continue
    if wfi.request['Memory'] > 2000: continue
    
    errors = wfi.getWMErrors(cache=10000)
    stats = wfi.getWMStats(cache=10000)
    
    f_errorcode=50660
    for task in errors:
        what = 'jobfailed'
        if not what in errors[task]:
            print "no",what,"in",task
            continue
        for errorcode in errors[task][what]:
            if f_errorcode!=None and str(f_errorcode)!=str(errorcode): continue
            counts = 0
            for site in errors[task][what][errorcode]:
                count = errors[task][what][errorcode][site]['errorCount']
                counts += count
            print task,what,errorcode,counts
"""


"""
wfn = 'pdmvserv_TOP-RunIISummer15wmLHEGS-00040_00159_v0__160919_161901_6135'
wfi = workflowInfo(url, wfn) 

now = time.mktime(time.gmtime())

f_errorcode=99303
##
#errors = wfi.getWMErrors()
errors = json.loads(open('errors.json').read())
for task in errors:
    ##keys
    failed = 'jobfailed'
    cooloff = 'jobcooloff'
    success = 'success'
    #print task,errors[task].keys()
    ## keys are error codes
    what= failed
    for errorcode in errors[task][what]:
        if f_errorcode!=None and str(f_errorcode)!=str(errorcode): continue
        for site in errors[task][what][errorcode]:
            count = errors[task][what][errorcode][site]['errorCount']
            samples = errors[task][what][errorcode][site]['samples']
            sample = samples[0] ## there isn't more anyways
            history = sample['state_history']
            last = max([h['timestamp'] for h in history])
            if (now-last)<(1*24*60*60):
                print count,errorcode,site,task,"less than an hour ago"
                print json.dumps(sample, indent=2)
                agent = sample['agent_name']
                wmbs = sample['wmbsid']
                wf = sample['workflow']
                os.system('ssh %s /afs/cern.ch/user/v/vlimant/scratch0/ops/central_ops/WmAgentScripts/bumbo.sh %s %s'%( agent, wf, wmbs ))

"""


#wfn = 'vlimant_TOP-RunIISpring16DR80-00063_00607_v0__160913_000439_4460'
#wfi = workflowInfo(url, wfn)

#open('sum.json','w').write(json.dumps( wfi.getSummary(), indent=2))
#open('error.json','w').write(json.dumps( wfi.getWMErrors(), indent=2))

#vdfv

"""
wfn = 'pdmvserv_TOP-RunIISummer15wmLHEGS-00044_00159_v0__160919_161910_4551'
wfi = workflowInfo(url, wfn)

#print "go"
#print json.dumps( wfi.getDashboard(since=1, sortby='appexitcode',site='T2_US_Caltech'), indent=2)
#open('dash.json','w').write( json.dumps(wfi.getFullPicture(since=1), indent=2))
#dash = json.loads(open('dash.json').read())
dash = wfi.getFullPicture(since=1)
all_codes = set()
for site,r in dash.items():
    #info = dash[site]
    #r = dict([(d['name'],d) for d in info])
    #dash[site] = r 
    all_codes.update( r.keys() )


print all_codes
all_codes = sorted(all_codes)
ht=open('../www/dash.html','w')
ht.write("<html>%s<br><table border=1 align=center><thead><tr>"%(wfn))
ht.write('<td>Site</td>')
for code in all_codes:
    ht.write('<td>%s</td>'%code)

ht.write('</tr></thead>\n')
def show(i):
    cell=''
    for p in ['cpu','running','pending',
              'aborted',
              'app-failed',
              'app-succeeded',
              ]:
        if p in i and i[p]:
            cell += '%s : %d <br>'%(p,i[p])
    return cell

for site,info in dash.items():
    ht.write('<tr><td>%s</td>\n'%site)
    for code in all_codes: 
        c =''
        dec=''
        if code in info: c = show(info[code])
        if code==0:
            dec='bgcolor=lightgreen'
        ht.write('<td %s>%s</td>'%(dec,c))
    ht.write('</tr>\n')
ht.write('</table></html>')
ht.close()
"""


#wfi = workflowInfo(url, 'vlimant_TOP-RunIISpring16DR80-00063_00607_v0__160913_000439_4460')
#print json.dumps( wfi.getRecoveryDoc(), indent=2)

#print json.dumps(getDatasetFileLocations(url,'/TT_TuneCUETP8M1_mtop1715_13TeV-powheg-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM  /TT_TuneCUETP8M1_mtop1715_13TeV-powheg-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM'),indent=2)
cdafd
"""
si = siteInfo()

banned= ['T2_US_Vanderbilt']

old=json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/equalizor.json').read())
for wfn in ['vlimant_BPH-RunIISummer15GS-Backfill-00030_00212_v0__160906_122234_1944',
            'vlimant_BPH-RunIISummer15GS-Backfill-00030_00212_v0__160907_112626_808',
            'pdmvserv_HIG-RunIISummer15wmLHEGS-00418_00157_v0__160909_001621_321',
            'pdmvserv_HIG-RunIISummer15wmLHEGS-00420_00157_v0__160909_001612_2018',
            'pdmvserv_HIG-RunIISummer15wmLHEGS-00415_00157_v0__160909_001628_2566'
            ]:
    wfi = workflowInfo(url, wfn)
    new_sites = sorted(list((set(wfi.request['SiteWhitelist']) - set(banned)) & set(si.sites_ready)))
    for task in wfi.getWorkTasks():
        if task.taskType in 'Production':
            
            bit={wfn : { task.pathName : {"ReplaceSiteWhitelist" : new_sites
                                          }}}
            print json.dumps( bit, indent=2)
            old["modifications"].update( bit )
open('/afs/cern.ch/user/c/cmst2/www/unified/equalizor.json','w').write( json.dumps( old, indent=2))

"""
#print getDatasetEventsPerLumi('/HIFlowCorr/HIRun2015-v1/RAW')
#print getDatasetEventsPerLumi('/HIMinimumBias2/HIRun2015-v1/RAW')

#print json.dumps(dict( workflowInfo(url, 'vlimant_BPH-RunIISummer15GS-Backfill-00030_00212_v0__160907_112626_808').getAgents()), indent=2)
#print json.dumps(dict( workflowInfo(url, 'vlimant_BPH-RunIISummer15GS-Backfill-00030_00212_v0__160906_122234_1944').getAgents()), indent=2)
#print json.dumps(dict( workflowInfo(url, 'vlimant_HIG-RunIISpring16DR80-Backfill-01832_00657_v0__160907_112339_3016').getAgents()), indent=2)
#print dict( workflowInfo(url, 'fabozzi_HIRun2015-HIFlowCorr-25Aug2016_758p5_160826_162420_2962').getAgents())
#print dict( workflowInfo(url, 'fabozzi_HIRun2015-HIMinimumBias2-25Aug2016_758p5_160826_162405_3905').getAgents())

"""
#wfi = workflowInfo(url, 'fabozzi_HIRun2015-HIFlowCorr-25Aug2016_758p5_160826_162420_2962')
#wfi = workflowInfo(url, 'fabozzi_HIRun2015-HIMinimumBias2-25Aug2016_758p5_160826_162405_3905')
wfi = workflowInfo(url, 'vlimant_BPH-RunIISummer15GS-Backfill-00030_00212_v0__160907_112626_808') 
wqe = wfi.getWorkQueueElements()
still_to_go=set()
done=set()
print len(wqe)
for wq in wqe:
    keys = wq['Inputs'].keys()
    if not keys:
        keys = ['MC']
    if wq['Status'] != 'Done':
        still_to_go.update( keys )
    else:
        done.update( keys )
print list(still_to_go)
print list(done)
"""

"""
wfs = getWorkflows(url, status='new',details=True)

for wf in wfs:
    if len(wf['RequestDate'])!=6:
        print wf['RequestDate'],wf['RequestName']
        continue
    Y,M,D,h,m,s = wf['RequestDate']
    if M<=9 and D<=3:
        print wf['RequestName'],wf['RequestStatus']
        reqMgrClient.invalidateWorkflow(url,  wf['RequestName'],  wf['RequestStatus'])
"""

#wfi = workflowInfo(url, 'vlimant_task_BPH-RunIIFall15DR76-00057__v1_T_160905_213311_4199')
#print wfi.acquisitionEra()
#print wfi.processingString()
#print wfi.getIO()

#print json.dumps( wfi.getRecoveryDoc(), indent=2)
#a= getDatasetLumis('/NoBPTX/Run2016C-TkAlCosmicsInCollisions-12Aug2016-v1/ALCARECO')

#print a

#print _,_,ph,db = getDatasetFiles(url, ''
#wfi = workflowInfo(url, 'pdmvserv_HIG-RunIISummer15wmLHEGS-00482_00150_v0__160823_201915_1419')

#print wfi.getWMStats()
#open('pdmvserv_HIG-RunIISummer15wmLHEGS-00482_00150_v0__160823_201915_1419.err','w').write( json.dumps( wfi.getWMErrors(), indent=2))

#wm = json.loads(open('pdmvserv_HIG-RunIISummer15wmLHEGS-00482_00150_v0__160823_201915_1419.err').read())


#print '\n'.join(getWorkflowByOutput(url, '/DoubleEG/Run2015B-LogError-08Jun2016-v1/RAW-RECO'))
#print 
#print '\n'.join(getWorkflowByOutput(url, '/DoubleEG/Run2015D-LogError-08Jun2016-v1/RAW-RECO'))


#de,abn = getDatasetDestinations(url, '/Neutrino_E-10_gun/RunIISpring15PrePremix-PU2016_80X_mcRun2_asymptotic_v14-v2/GEN-SIM-DIGI-RAW')
#print de.keys()

#print getDatasetEventsAndLumis('/AlCaLumiPixels3/Run2016B-v2/RAW')
#print getDatasetEventsAndLumis('/AlCaLumiPixels3/Run2016B-ALCARECOLumiPixels-07Jul2016-v3/ALCARECO')

"""
wfs = getWorkflows(url, status='completed', rtype='Resubmission',details=True)

collect = defaultdict(lambda : defaultdict(int))
for wf in wfs:
    task=wf['InitialTaskPath'].split('/')[-1]
    collect[wf['Campaign']][task
    wf['PrepID']
    print task
    break

"""
"""
dataset = '/BTagCSV/Run2016B-01Jul2016-v2/AOD'
delete_at_site = 'T1_US_FNAL_Disk'
loc = getDatasetFileLocations(url, dataset)

for f in loc:
    #print f
    #print json.dumps(sorted(loc[f]), indent=2)
    if len(loc[f]) == 1:
        print "only one copy of",f
    break
"""
#o= dbs3Client.duplicateRunLumi('/BTagCSV/Run2016B-01Jul2016-v2/AOD', verbose=True, skipInvalid=True)
#o= dbs3Client.duplicateRunLumi('/BdToJpsiKPi_BFilter_MSEL5_TuneZ2star_8TeV-Pythia6-evtgen/Summer12DR53X-PU_RD2_START53_V19F_ext4-v1/AODSIM', verbose=True, skipInvalid=True)
#o= dbs3Client.duplicateRunLumi('/HLTPhysics/Run2016B-TkAlMinBias-01Jul2016-v2/ALCARECO', verbose=True, skipInvalid=True)
#o= dbs3Client.duplicateRunLumi('/VectorMonoW_Mphi-10000_Mchi-1_gSM-0p25_gDM-1p0_v2_13TeV-madgraph/RunIISummer15wmLHEGS-MCRUN2_71_V1-v1/LHE', verbose=True, skipInvalid=True)
#print o

"""

lib = '/Neutrino_E-10_gun/RunIISpring15PrePremix-PU2016_80X_mcRun2_asymptotic_v14-v2/GEN-SIM-DIGI-RAW'

SI = siteInfo()
chops,sizes = getDatasetChops(lib, 4000)

#europ = [site for site in SI.sites_ready if any([loc in site for loc in ['_DE_','_IT_','_FR_','_UK_'])]
europ = [site for site in SI.sites_ready if not any([loc in site for loc in ['_US_']])]
us = [site for site in SI.sites_ready if any([loc in site for loc in ['_US_']])]

print sorted( us )
europ = [site for site in europ if SI.disk[SI.CE_to_SE(site)]]
us = [site for site in us if SI.disk[SI.CE_to_SE(site)]]
print sorted( us )

veto=['T2_US_Vanderbilt','T1_US_FNAL']
for v in veto:
    if v in us:
        us.remove( v )


print sorted( us )

europ = [site for site in europ if site in SI.sites_mcore_ready]
us = [site for site in us if site in SI.sites_mcore_ready]

print sorted( us )

presence = getDatasetPresence(url, lib)
there = [SI.SE_to_CE(s) for s in presence.keys() if presence[s][0]]
print there 

print sorted( us )

europ = [site for site in europ if not site in there]
us = [site for site in us if not site in there]
print europ, sum([SI.disk[SI.CE_to_SE(s)] for s in europ])
print us, sum([SI.disk[SI.CE_to_SE(s)] for s in us])

spreading = distributeToSites(chops, europ,  n_copies = 1, weights=SI.disk, sizes=sizes)
for dest,blocks in spreading.items():
    print len(blocks),"for",dest
    

spreading = distributeToSites(chops, us,  n_copies = 1, weights=SI.disk, sizes=sizes)
for dest,blocks in spreading.items():
    print len(blocks),"for",dest
    makeReplicaRequest(url, dest, blocks, "Spreading of MIX library")
"""

#print json.dumps(spreading, indent=2)

#wfi = workflowInfo(url, sys.argv[1] )
#print json.dumps(wfi.getAgents(), indent=2)


"""
wfs = []
wfs.extend( getWorkflows(url, 'acquired', details=True) ) 
wfs.extend( getWorkflows(url, 'running-open', details=True) )
wfs.extend( getWorkflows(url, 'running-closed', details=True) )

wfs.sort( key = lambda i : i['RequestPriority'] ,reverse=True)
wfs = filter(lambda i: not '_RV' in i['RequestName'], wfs)
bip = set()

rel = defaultdict(lambda : defaultdict(int))
for wf in wfs:
    if wf['RequestType'].startswith('MonteCarlo'):
        rel[wf['Campaign']][wf['CMSSWVersion']] += 1
        print wf['Campaign']
    continue
    wfi = workflowInfo(url, wf['RequestName'], request=wf)
    #print wfi.request['RequestPriority]
    wq = wfi.getWorkQueue()
    wqe = [w[w['type']] for w in wq]
    #print json.dumps( wqe, indent=2)
    for wq in wqe:
        if wq['ChildQueueUrl'] and '311' in wq['ChildQueueUrl']:
            if not  wfi.request['RequestName'] in bip:
                print wfi.request['RequestName'], wfi.request['RequestPriority']
                bip.add(  wfi.request['RequestName'] )
            
print json.dumps( rel ,indent=2)


"""

#presence = getDatasetPresence(url, '/Neutrino_E-10_gun/RunIISpring15PrePremix-PU2016_80X_mcRun2_asymptotic_v14-v2/GEN-SIM-DIGI-RAW')
#presence = getDatasetPresence(url, '/TTJets_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM')
#print json.dumps( presence, indent=2)

#wfi = workflowInfo(url, sys.argv[1] )
#collect = wfi.getJobs()
#open('.%s.agent.json'%(wfi.request['RequestName']),'w').write( json.dumps( collect ,indent=2 ) )

#print checkTransferLag( url, 646426, ['/X53X53To2L2Nu_M-800_RH_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIISpring16DR80-PUSpring16RAWAODSIM_80X_mcRun2_asymptotic_2016_v3-v1/RAWAODSIM'])

"""
dataset='/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-FTFP_BERT_MCRUN2_71_V1-v1/GEN-SIM'
usors = getWorkflowByMCPileup(url, dataset, details=True)
now = time.mktime(time.gmtime())
for usor in usors:
    if len(usor['RequestDate'])!=6:continue
    d =time.mktime(time.strptime("-".join(map(lambda n : "%02d"%n, usor['RequestDate'])), "%Y-%m-%d-%H-%M-%S"))
    print d
    delay_days = 30
    delay = delay_days*24*60*60
    if (now-d)>delay:
        print "unlocking secondary input after",delay_days,"days"
"""

"""
remainings ={}
site = 'T1_UK_RAL_Disk'
site = sys.argv[1]
remainings[site] = json.loads(open('%s/remaining_%s.json'%(monitor_dir,site)).read())
if True:
    if True:

        ld = remainings[site].items()
        ld.sort( key = lambda i:i[1]['size'], reverse=True)
        table = "<html>Updated %s GMT, <a href=remaining_%s.json>json data</a><br>"%(time.asctime(time.gmtime()),site)

        accumulate = defaultdict(lambda : defaultdict(float))
        for item in remainings[site]:
            tier = item.split('/')[-1]

            for reason in remainings[site][item]['reasons']:
                accumulate[reason][tier] += remainings[site][item]['size']
        table += "<table border=1></thead><tr><th>Reason</th><th>size [TB]</th></thead>"
        for reason in accumulate:
            s=0
            table += "<tr><td>%s</td><td><ul>"% reason
            subitems = accumulate[reason].items()
            subitems.sort(key = lambda i:i[1], reverse=True)

            for tier,ss in subitems:
                table += "<li> %s : %10.3f</li>"%( tier, ss/1024.)
                s+=  ss/1024.
            table+="</ul>total : %.3f</td>"%s

        table += "</table>\n"
        table += "<table border=1></thead><tr><th>Dataset</th><th>Size [GB]</th><th>Label</th></tr></thead>\n"
        for item in ld:
            table+="<tr><td>%s</td><td>%d</td><td><ul>%s</ul></td></tr>\n"%( item[0], item[1]['size'], "<li>".join([""]+item[1]['reasons']))
        table+="</table></html>"
        open('%s/remaining_%s.html'%(monitor_dir,site),'w').write( table )
        
""" 

#l= getWorkflowByMCPileup(url,'/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-FTFP_BERT_MCRUN2_71_V1-v1/GEN-SIM',details=True)
#print [(i['RequestName'],i['RequestStatus']) for i in l]

#wf = workflowInfo(url, 'prozober_TOP-RunIISummer15GS-Backfill-00089_00415_v0__160725_121858_75')
#wfo = session.query(Workflow).filter(Workflow.name=='prozober_TOP-RunIISummer15GS-Backfill-00089_00415_v0__160725_121858_75').first()
#print (wfo or wf.request['RequestType']=='Resubmission')


#site = sys.argv[1]
#site = 'T1_DE_KIT_Disk'

#l = getUnsubscribedBlocks(url, site)
#print json.dumps( l , indent=2 )


#print makeReplicaRequest(url, site, l , comments="subscribed to dataops blocks which obviously were produced by dataops over time", mail=False, approve=True )


#print getDatasetPresence(url, '/WJetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer15wmLHEGS-MCRUN2_71_V1_ext1-v2/GEN-SIM ')
"""
site = sys.argv[1]#'T1_ES_PIC_Disk'
r=json.loads(open('%s/remaining_%s.json'%(monitor_dir,site)).read())
ld=r.items()
ld.sort( key = lambda i:i[1]['size'], reverse=True)    
table = "<html>Updated %s GMT, <a href=remaining_%s.json>json data</a><br><table border=1></thead><tr><th>Dataset</th><th>Size [GB]</th><th>Label</th></tr></thead>\n"%(time.asctime(time.gmtime()),site)
for item in ld:
    table+="<tr><td>%s</td><td>%d</td><td><ul>%s</ul></td></tr>\n"%( item[0], item[1]['size'], "<li>".join([""]+item[1]['reasons']))
table+="</table></html>"
open('%s/remaining_%s.html'%(monitor_dir,site),'w').write( table )
"""
fdsg
#out = '/Cosmics/Commissioning2015-TkAlCosmics0T-01Mar2016-v3/ALCARECO'
#dbsf,phf,missing_phedex,missing_dbs  = getDatasetFiles(url, out)
#print len(dbsf)
#print len(phf)
#print missing_dbs
#print missing_phedex

#usors = getWorkflowByMCPileup(url, '/Neutrino_E-10_gun/RunIISpring15PrePremix-AVE_50_BX_25ns_76X_mcRun2_asymptotic_v12-v3/GEN-SIM-DIGI-RAW', details=True)
usors = getWorkflowByMCPileup(url, '/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-MCRUN2_71_V1-v1/GEN-SIM', details=True)

for usor in usors:
    print usor['RequestDate'],usor['RequestName'],usor['RequestStatus']

#print json.dumps( getDatasetPresence(url,'/VBF_HToZZTo4L_M150_13TeV_powheg2_JHUgenV6_pythia8/RunIISpring16DR80-PUSpring16RAWAODSIM_80X_mcRun2_asymptotic_2016_v3-v1/RAWAODSIM'), indent=2)


#print json.dumps(checkTransferStatus(url, 676392, nocollapse=True), indent=2)
#print json.dumps(getWorkflowById(url, 'ReReco-Run2015C_25ns-boff-09Jun2016-0004'), indent=2)

#wfs = session.query(Workflow).all()
#print len(wfs)
#session.delete(wfo)
#session.commit()


#print getDatasetEventsPerLumi('/AlCaLumiPixels1/Run2016B-v2/RAW')

#fsgf

#wfi = workflowInfo(url, 'pdmvserv_HIN-pPb816Spring16GS-00001_00001_v0__160622_161215_3869')
#print wfi.go()

#wfs = getWorkflows(url, 'assignment-approved', details=True, user='pdmvserv')
#for wf in wfs:
#    if 'TOP-RunIISpring16MiniAODv2-00039' in wf['RequestName']:
#        print wf['RequestName']

sys.exit(10)

for pid in filter(None,open('antanas.txt').read().split('\n')):
    wfs = getWorkflowById(url, pid, details=True)
    print len(wfs)
    expected_outputs= None
    main_wf = None
    for wf in wfs:
        if not wf['RequestStatus'] in ['announced','normal-archived']: continue
        if wf['RequestType'] == "Resubmission" : continue
        expected_outputs = set( wf['OutputDatasets'])
        main_wf = wf
    if not  main_wf: 
        print "left",pid
        continue
    found_bad=False
    #print expected_outputs
    print main_wf['RequestName']
    wfs = getWorkflowById(url, main_wf['PrepID'], details=True)
    for wf in wfs:
        #print wf['RequestName']
        if not wf['RequestStatus'] in ['announced','normal-archived']: continue
        if wf['RequestType'] != "Resubmission" : continue
        outs= set(wf['OutputDatasets'])
        #print outs
        if not outs.issubset( expected_outputs ):
            print "BAD",wf['RequestName']
            print ','.join(outs-expected_outputs)
            found_bad=wf
            print "invalidating",found_bad['RequestName'],found_bad['RequestStatus']
            s= raw_input("invalidating "+found_bad['RequestName']+" "+found_bad['RequestStatus'])
            if s=='y':
                reqMgrClient.invalidateWorkflow(url, found_bad['RequestName'], found_bad['RequestStatus'])



#dbsapi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

#l=dbsapi.listFiles(dataset='/RSGravTohhTohVVhbbToVVfullLep_narrow_M-1000_13TeV-madgraph/RunIISpring16MiniAODv2-PUSpring16RAWAODSIM_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM')
#lfn='/'.join(l[0]['logical_file_name'].split('/')[:3])
#print lfn
#print "valid", dbs3Client.getFileCountDataset( '/RSGravTohhTohVVhbbToVVfullLep_narrow_M-1000_13TeV-madgraph/RunIISpring16MiniAODv2-PUSpring16RAWAODSIM_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM')
#print "invalid", dbs3Client.getFileCountDataset( '/RSGravTohhTohVVhbbToVVfullLep_narrow_M-1000_13TeV-madgraph/RunIISpring16MiniAODv2-PUSpring16RAWAODSIM_80X_mcRun2_asymptotic_2016_miniAODv2_v0-v1/MINIAODSIM', onlyInvalid=True)
#print json.dumps(l, indent=2)

#wfs= session.query(Workflow).filter(Workflow.status=='trouble').all()
#l = json.loads(open('/afs/cern.ch/user/v/vlimant/public/ops/onhold.json').read())
#for w in l:
#    wfo = session.query(Workflow).filter(Workflow.name == w).first()
#    if wfo:
#        wfo.status ='forget'
#session.commit()

#wfi = workflowInfo(url, 'fabozzi_Run2015A-DoubleEG-boff-09Jun2016_765p1_160609_001324_7641')
#print wfi.go()

sys.exit(2)
wfs = []
rtype=None
#wfs.extend( getWorkflows(url, 'running-closed', user=None, rtype=rtype ,details=True)) 
#wfs.extend( getWorkflows(url, 'running-open', user=None, rtype=rtype ,details=True)) 
#wfs.extend( getWorkflows(url, 'completed', user=None, rtype=rtype ,details=True)) 
#wfs.extend( getWorkflows(url, 'closed-out', user=None, rtype=rtype ,details=True)) 
#wfs.extend( getWorkflows(url, 'announced', user=None, rtype=rtype ,details=True)) 


wfs = json.loads(open('wfs.json').read())
#wfs.extend( getWorkflows(url, 'normal-archived', user=None, rtype=rtype ,details=True)) 
for wf in wfs:
    if not any([wf['RequestName'].startswith(n) for n in ['prozober','jen']]):        continue
    if not 'MergedLFNBase' in wf:
        print '\t missing',wf['RequestName'],wf['RequestStatus']
        continue

    if 'back' in wf['MergedLFNBase']:
        print wf['RequestName'],wf['RequestStatus'],wf['RequestType']
        if 'InitialTaskPath' in wf:
            print wf['InitialTaskPath'].split('/')[-1]
 

    if 'InitialTaskPath' in wf:
        if 'task_' in wf['RequestName']: 
            continue
        w = wf['InitialTaskPath'].split('/')[-1]
        if 'back' in wf['MergedLFNBase']:
            print "BAD ",w, wf['RequestName'],wf['RequestStatus']
            if 'merge' in w.lower():
                print "\t merge"
            else:
                print "\t\t not merge"
        else:
            print "GOOD",w, wf['RequestName'],wf['MergedLFNBase']

#open('wfs.json','w').write( json.dumps( wfs ))

#setDatasetStatus('/QCD_HT200to300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16DR80-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/AODSIM','VALID')
#setDatasetStatus('/QCD_HT200to300_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM','VALID')
#wfi = workflowInfo(url, 'pdmvserv_task_SMP-RunIISpring16DR80-00003__v1_T_160407_160601_5633')
#print wfi.getRequestNumEvents()

#si = siteInfo()
#si.pick_dSE
#for wfo in session.query(Workflow).filter(Workflow.status == 'away').all():
#    wfi = workflowInfo(url, wfo.name)
#    if wfi.request['RequestStatus'] == 'assignment-approved':
#        print wfo.name
#        wfo.status = 'considered'

#session.commit()
sys.exit(2)
#conn  =  httplib.HTTPConnection('localhost', port='5984')#, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
r1 = conn.request("GET",'/workqueue/_design/WorkQueue/_rewrite/elementsInfo?request=pdmvserv_task_B2G-RunIISpring16DR80-00527__v1_T_160501_103420_9369')
r2=conn.getresponse()
t=r2.read()
print t
result = json.loads(r2.read())

print result

#CI = campaignInfo()
#wfi = workflowInfo(url, 'pdmvserv_SUS-RunIISpring16wmLHEFSPremix-00001_00003_v1_WFTest_160422_151822_8644')
#print wfi.getCampaigns()
#for campaign in wfi.getCampaigns():
#    if not CI.go( campaign ):
#        print "boh",campaign
#print json.dumps( wfi.getWMStats()["AgentJobInfo"] , indent=2 )
#print wfi.getComputingTime()

#print wfi.getIO()
#print wfi.getCampaigns()
#si = siteInfo()
#print si.disk['T0_CH_CERN_Disk']


#print getDatasetPresence(url, '/TT_Mtt-700to1000_TuneCUETP8M1_13TeV-powheg-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM')
#print getDatasetBlocksFraction(url, '/TT_Mtt-700to1000_TuneCUETP8M1_13TeV-powheg-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM')
#gfag
#gfsdg
#print getWorkflows(url, 'assignment-approved', user='pdmvserv')

#fral


#    for s in 

#bs=set()
#for wfo  in session.query(Workflow).filter(Workflow.status == 'close-unlock').all():
#    print wfo.name

#for wfo  in session.query(Workflow).filter(Workflow.status.endswith('unlock')).all():
#    bs.add( wfo.status.split('-')[0])
    #wfo.status = 'staging'
#print bs

#session.commit()


#cache = getWorkflows(url,'assignment-approved', details=True)
#print len(cache)

#si =siteInfo()
#print si.total_disk('quota')
#print si.total_disk('disk')
#print si.total_disk('locked')

#print json.dumps( si.sites_pressure, indent=2)

#wfi = workflowInfo(url, 'pdmvserv_task_TOP-RunIISpring16DR80-00001__v1_T_160331_151408_3872')
#print wfi.getMulticore()

#nl = newLockInfo()
#nl.lock('/Neutrino_E-10_gun/RunIISpring15PrePremix-AVE_25_BX_25ns_76X_mcRun2_asymptotic_v12-v3/GEN-SIM-DIGI-RAW')
#nl.lock('/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISummer15GS-MCRUN2_71_V1_ext1-v2/GEN-SIM')
#nl.release('/DM_PseudoscalarWH_Mphi-1000_Mchi-100_gSM-1p0_gDM-1p0_13TeV-JHUGen/RunIISpring15MiniAODv2-Asympt25ns_74X_mcRun2_asymptotic_v2-v1/MINIAODSIM')
#nl.release('/MinBias_TuneZ2star_8TeV-pythia6/Summer12-START50_V13-v3/GEN-SIM')

#wfi = workflowInfo(url, 'pdmvserv_EXO-RunIIFall15MiniAODv1-01496_00074_v1__160321_162248_1133')
#res = reqMgrClient.closeOutWorkflowCascade(url, wfi.request['RequestName'])
#print res

#for wf in filter(None,open('del.txt').read().split('\n')):
#    wfo = session.query(Workflow).filter(Workflow.name == wf).first()
#    if wfo:
#        print wfo.name
#        session.delete( wfo )
#session.commit()

#workflows = getWorkflows(url, status="assignment-approved", user="pdmvserv")
#workflows = getWorkflows(url, status="assignment-approved", user='fabozzi', rtype="ReReco")
#print json.dumps(workflows, indent=2)

#wfi = workflowInfo(url, 'pdmvserv_TOP-pp502Fall15wmLHEGS-00001_00002_v0__160305_075108_8467')
#print wfi.getSchema()

sys.exit(3)

#wf = sys.argv[1]
#reqMgrClient.closeOutWorkflow(url, wf)
#reqMgrClient.announceWorkflow(url, wf)
#sys.exit(0)

#sendLog('bli','bla-bfab-gfad')
#fhlsd
#pending = listCustodial(url, site='T1_US_FNAL_MSS')
#print pending

#grsg
#checks = checkTransferStatus(url, 570031, nocollapse=True)
#print checks


o = searchLog( sys.argv[1] )
for i in o:
    print "-"*10,i['_source']['subject'],"-"*2,i['_source']['date'],"-"*10
    print i['_source']['text']

sys.exit(1)


#for wfo in session.query(Workflow).filter(Workflow.status == 'considered-tried').all():
#    print wfo.name

"""
for phedexid in filter(None,open('setback_phedexid_to_positive.txt').read().split('\n')):
    pos = session.query(Transfer).filter(Transfer.phedexid == int(phedexid)).all()
    neg = session.query(Transfer).filter(Transfer.phedexid == -int(phedexid)).all()
    if not pos:
        if neg:
            print "should switch back",phedexid
            for tr in neg:
                tr.phedexid = -tr.phedexid
                session.commit()
                pass
        else:
            print "absent"
"""

""" 
were_old_staging = filter(None,open('were_staging.txt').read().split('\n'))
were_new_staging = filter(None,open('were_OVERset_staging.txt').read().split('\n'))    
latching = filter(None,open('latching.txt').read().split('\n'))
latching_and_transfer = filter(None,open('latching_and_transfer.txt').read().split('\n'))
were_injected_1 = filter(None,open('were_injected_1.txt').read().split('\n'))
were_injected_2 = filter(None,open('were_injected_2.txt').read().split('\n'))

for wfo in session.query(Workflow).filter(Workflow.status == 'considered-tried').all():
    if wfo.name in were_old_staging:
        print "old staging"
        wfo.status = 'staging'
        session.commit()
    elif wfo.name in were_new_staging:
        print "new staging"
        wfo.status = 'staging'
        session.commit()
    elif wfo.name in latching:
        print "is latching only"
        wfo.status = 'staging'
        session.commit()
    elif wfo.name in latching_and_transfer:
        print "is latching"
        wfo.status = 'staging'
        session.commit()
    elif wfo.name in were_injected_1:
        print "recently injected 1"
    elif wfo.name in were_injected_1:
        print "recently injected 2"
    else:
        print "\t\tnowhere",wfo.name
"""

SI = siteInfo()

transfers = [#598501,
             598508, 
             #598509, 
             #598510, 
             #598513,
             #598517
             ]
mild = [
    #598511,
    598516
    ]

## use the cache
fresh = True
recent = False

items = []
for transfer in sorted(transfers+mild):
    print transfer
    cfile = '%s.sub.json'%(transfer)
    if os.path.isfile( cfile) and not fresh:
        result = json.loads( open(cfile).read())
    else:
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1 = conn.request("GET",'/phedex/datasvc/json/prod/subscriptions?request=%s&collapse=n&create_since=0'% transfer)
        r2=conn.getresponse()
        result = json.loads(r2.read())
        open(cfile,'w').write( json.dumps( result , indent=2))

    for dataset in result['phedex']['dataset']:
        by_dataset=False
        size = dataset['bytes']
        for sub in dataset.get('subscription',[]):
            #print sub
            if sub['level'] == 'DATASET':
                to = sub['node']
                items.append( {'node' : sub['node'],
                               'dataset' : dataset['name'],
                               'suspended' : sub['suspended'],
                               'percent' : sub['percent_bytes'],
                               'size' : sub['node_bytes'] / 1024.**4,
                               'bytes' : size / 1024.**4
                               })
                by_dataset=True
        for block in dataset.get('block',[]):
            size = block['bytes']
            for sub in block['subscription']:
                if sub['level'] == 'BLOCK' or not by_dataset:
                    to = sub['node']
                    items.append( {'node' : sub['node'],
                                   'block' : block['name'],
                                   'dataset' : dataset['name'],
                                   'suspended' : sub['suspended'],
                                   'percent' : sub['percent_bytes'],
                                   'size' : sub['node_bytes'] / 1024.**4,
                                   'bytes' : size / 1024.**4
                                   })

    #print json.dumps(items, indent=2)


wf_by_using = {}
priority_by_using = json.loads(open('priority_by_using.json').read())
if fresh: priority_by_using = {}
dont_touch = set(json.loads(open('dont_touch.json').read()))
if fresh:  dont_touch =set()
size_per_item_per_site = defaultdict( lambda : defaultdict (int))
bytes_per_item_per_site = defaultdict( lambda : defaultdict (int))

for item in items:
    dataset = item['dataset']
    bytes_per_item_per_site[item['node']][dataset] += item['bytes']
    size_per_item_per_site[item['node']][dataset] += item['size']

    if not dataset in wf_by_using and (fresh):
        print "who is using",dataset
        wfs = getWorkflowByInput(url, dataset, details=True)
        wfs = [ wf for wf in wfs if wf['RequestStatus'] == 'assignment-approved']
        wf_by_using[dataset] = [wf['RequestName'] for wf in wfs]
        if wfs:
            priority_by_using[dataset] = max([wf['RequestPriority'] for wf in wfs])
        else:
            dont_touch.add( dataset )

open('priority_by_using.json','w').write( json.dumps( priority_by_using, indent=2 ))
open('dont_touch.json','w').write( json.dumps( list( dont_touch ), indent=2))

no_go = defaultdict(set)
for site in bytes_per_item_per_site:
    #print site
    available = SI.disk[site]
    available *= 0.9 ## 80% of available
    priority_ordered = priority_by_using.items()

    def prio_and_size( i, j):
        if i[1]==j[1]:
            return cmp( bytes_per_item_per_site[site].get(j[0],0.), bytes_per_item_per_site[site].get(i[0],0.))
        else:
            return cmp( i[1], j[1] )
    priority_ordered.sort( cmp = prio_and_size , reverse=True)
    last_prio = None
    for dataset,prio in priority_ordered:
        if dataset in dont_touch:
            print "not relevant is",dataset
            no_go[site].add( dataset )
        #print dataset,prio
        if dataset in bytes_per_item_per_site[site]:
            if available-bytes_per_item_per_site[site][dataset]>0:
                if last_prio==None or prio>=last_prio:
                    print "remaining",available
                    print "prio",prio,last_prio
                    print "passing",dataset,bytes_per_item_per_site[site][dataset],priority_by_using[dataset]
                    available -= bytes_per_item_per_site[site][dataset]
                else:
                    no_go[site].add( dataset )
                    last_prio = max(prio,last_prio)
                    print "no go",dataset,prio,last_prio

            else:
                no_go[site].add( dataset )
                last_prio = max(prio,last_prio)
                print "no go",dataset,prio,last_prio
  
    print "no go at",site
    print json.dumps(list(no_go[site]), indent=2)


#print json.dumps(priority_by_using, indent=2)

for item in items:
    #print item
    dataset = item['dataset']
    site = item['node']
    tier = dataset.split('/')[-1]
    if dataset in dont_touch: 
        print "no relevant wf for",dataset
        continue
    if dataset in no_go[site]:
        print "not fitting in space and time",dataset
        continue
    time.sleep(0.1)
    ph_item = item.get('block', item.get('dataset'))
    print "unsuspending",ph_item,"at",site
    if (recent or fresh) and item['suspended'] == 'n':
        print "already not suspended"
        continue ## we do not have 

    r=updateSubscription(url, site, ph_item, suspend=0)['phedex']
    print r

    continue
    if priority_by_using[dataset] >= 90000 or tier in ['LHE','AODSIM']:
        print "unsuspending",ph_item
        if (recent or fresh) and item['suspended'] == 'n':
            print "already not suspended"
            continue ## we do not have the live value, only cached
        r=updateSubscription(url, item['node'], ph_item, suspend=0)['phedex']
        if r.get('block', r.get('dataset')): print "\tadd effect"
        else: print "\thad NO effect",r
        #print r
    else:
        ### the assumption is that all are suspended, no need to re-suspend anything
        continue
        print "suspending",ph_item
        if (recent or fresh) and item['suspended'] == 'y':
            print "already suspended"
            continue ## we do not have the live value, only cached
        r=updateSubscription(url, item['node'], ph_item, suspend=9999999999)['phedex']
        if r.get('block', r.get('dataset')): print "\tadd effect"
        else: print "\thad NO effect",r
        #print r



#for wfo in session.query(Workflow).filter(Workflow.status=='away').filter(Workflow.name.contains('160206')).filter(Workflow.name.startswith('vlimant')).all():
#    if '06_10' in wfo.name or '06_16' in wfo.name:
#        print wfo.name

"""
pending=defaultdict(int)
for wfo in session.query(Workflow).filter(Workflow.status.startswith('assistance')).all():
    if 'custodial' in wfo.status: continue
    wfi = workflowInfo(url, wfo.name)
    pending [wfi.request['Campaign']] += wfi.request['TotalInputEvents']
    wfi.sendLog('test','counting manual')
    #wfi.flushLog()
    #sendLog('test','toc', wfi=wfi)
    break

print json.dumps( pending, indent=2 )
"""


#dbs3Client.duplicateRunLumi( '/QCD_Pt-120to170_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIIFall15MiniAODv1-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/MINIAODSIM', skipInvalid=True, verbose=True)
#dbs3Client.duplicateRunLumi( '/QCD_Pt-120to170_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIIFall15DR76-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/AODSIM', skipInvalid=True, verbose=True)

"""
conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
there = '/reqmgr2/data/request?status=completed&request_type=DQMHarvest&details=true'
r1=conn.request("GET", there, headers={"Accept":"application/json"})
r2=conn.getresponse()
result = json.loads(r2.read())['result'][0]
readys = [result[wfn]['InputDataset'] for wfn in filter(lambda f : '27Jan2016' in f, result.keys())]
print '\n'.join( readys )
"""

"""
SI =siteInfo()

wfi = workflowInfo(url, 'pdmvserv_task_B2G-RunIIFall15DR76-00808__v1_T_160121_122607_6357')
(_,primaries,_,secondaries,sites_allowed) = wfi.getSiteWhiteList(verbose=False) 
se_allowed = [SI.CE_to_SE(site) for site in sites_allowed]
se_allowed.sort()   
se_allowed_key = ','.join(se_allowed)
prim_where = set()
for need in list(primaries):
    presence = getDatasetPresence( url, need , within_sites=se_allowed)
    print need
    print presence
    prim_where.update( presence.keys() )

print prim_where
for need in list(secondaries): 
    presence = getDatasetPresence( url, need , within_sites=se_allowed)
    presence = dict([(k,v) for (k,v) in presence.items() if k in prim_where])
    
    print presence
"""

#for wfo in session.query(Workflow).filter(Workfow.status=='assistance-recovered-recovering').all():
#    wfo.status = 'assistance-recovering-recovered'
#session.commit()

#COI = closeoutInfo()
#COI.html()



#wfi = workflowInfo(url, 'fabozzi_Run2015D-SingleMuon-16Dec2015_763_151218_000333_2963', deprecated=True)
#hwfi = workflowInfo(url, 'vlimant_HARVEST-Run2015D-SingleMuon-16Dec2015_763_160127_144115_8436', deprecated=True)

#print json.dumps( parameters, indent=2)
#result = reqMgrClient.assignWorkflow(url, hwfi.request['RequestName'], team, parameters)
#print result

#print SI.sites_veto_transfer
#b,p,dd,pd = getDatasetFiles(url, sys.argv[1] , without_invalid=False)
#print len(b)
#print len(p)
#print dd
#print pd

#print len(session.query(Workflow).filter(Workflow.status.startswith('done')).all())
#print findLateFiles(url, '/RSGravToWW_width0p2_M-3000_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM', going_to = 'T1_IT_CNAF_Disk' )

#for wfo in session.query(Workflow).filter(Workflow.status == 'staged').all():
#    wfo.status = 'staging'
#session.commit()
"""
now = time.mktime( time.gmtime() )
for wf in getWorkflows(url, 'closed-out', user=None,details=True):
    wfi = workflowInfo(url, wf['RequestName'], request=wf)
    closedout_log = filter(lambda change : change["Status"] in ["closed-out"],wfi.request['RequestTransition'])
    closedout = closedout_log[-1]['UpdateTime']
    if (now - closedout) > (24.*60*60):
        print wf['RequestName'],"since", (now - closedout)/(60*60.),"more than a day"
    else:
        print wf['RequestName'],"since", (now - closedout)/(60*60.)
"""
#issues = checkTransferLag( url, 568108, ['/TTTo2L2Nu_13TeV-powheg/RunIISummer15GS-MCRUN2_71_V1_ext1-v2/GEN-SIM'])
#print json.dumps( issues, indent =2)
#wfi = workflowInfo(url, sys.argv[1])

#print wfi.getSiteWhiteList()
sys.exit(2)

wms = wfi.getWMStats()
wmss = wms['AgentJobInfo']
#print wmss.keys()
for agent in wmss:
    #print wmss[agent].keys()
    for task in wmss[agent]['tasks']:
        print agent, task
        print wmss[agent]['tasks'][task].keys()
        print wmss[agent]['tasks'][task]['status']

#print json.dumps( wms['AgentJobInfo'] , indent=2)
#locs =  wfi.getGQLocations()
#for b,loc in locs.items():
#    if not loc:
#        print b,"has no location for GQ in",wfi.request['RequestName']

#wfi = workflowInfo(url, 'dmason_BoogaBoogaBooga_151221_142741_7075')
#print wfi.getComputingTime()
#print wfi.firstTask()
#print wfi.getIO()
#print wfi.acquisitionEra()

sys.exit(1)

#wf = sys.argv[1]
#wfi = workflowInfo(url, wf)
#print json.dumps( wfi.getAgents() , indent=2)



"""
dss = DSS()
print dss.get_block_size( '/BlackHole_BH4_MD-8000_MBH-10000_n-6_TuneCUETP8M1_13TeV-charybdis/RunIIWinter15pLHE-MCRUN2_71_V1-v1/LHE' )

sys.exit(12)

SI = siteInfo()
cache = getWorkflows(url, 'acquired',details=True)
sites = defaultdict(int)
wmcpu = defaultdict(int)
mycpu = defaultdict(int)
for wfo in session.query(Workflow).filter(Workflow.status == 'away').all():
    #if not 'DR76' in wfo.name: continue
    cached = filter(lambda d : d['RequestName']==wfo.name, cache)
    if not cached: continue
    wfi = workflowInfo(url, wfo.name, request=cached[0])
    for site in wfi.request['SiteWhitelist']:
        wmcpu[site] += wfi.request['TotalEstimatedJobs']
        mycpu[site] += int(wfi.getComputingTime()/8.)
        sites[site] += 1

for site in SI.cpu_pledges:
    print SI.cpu_pledges[site],"slots at",site
    if site in sites: print sites[site],"wfs"
    if site in wmcpu: print wmcpu[site],"estimated jobs"
    if site in mycpu: print mycpu[site],"better estimated jobs"
    ## make a ratio to show pressure ?

#print json.dumps( dict(sites), indent=2)
#print json.dumps( dict(wmcpu), indent=2)
#print json.dumps( dict(mycpu), indent=2)

"""


#d,b = getDatasetDestinations(url, '/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIISummer15GS-MCRUN2_71_V1-v2/GEN-SIM')
#d,b = getDatasetDestinations(url, '/ADDdiPhoton_LambdaT-5000_Pt-500_TuneCUETP8M1_13TeV-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/AODSIM')
#print d
#print b

"""
if True:
    ds = sys.argv[1]
    print ds
    c,i= findCustodialCompletion(url,  ds) 
    print c
    b,f = findLostBlocksFiles(url,ds)
    if len(b) or len(f):
        print "\t",len(b),"blocks lost"
        print "\t",len(f),"files lost"
"""

sys.exit(3)

#ds = '/WJetsToLNu_HT-200To400_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9_ext1-v2/AODSIM'
ds = '/ADDdiPhoton_LambdaT-5000_Pt-500_TuneCUETP8M1_13TeV-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v2/AODSIM'
#ds = '/WRToNuMuToMuMuJJ_MW-4600_MNu-2300_TuneCUETP8M1_13TeV-pythia8/RunIIFall15DR76-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/AODSIM' # Nov 23 01:05:43 2015
#ds = '/SMS-T2tt_mStop-275_mLSP-75to200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15FSPremix-FastAsympt25ns_MCRUN2_74_V9-v1/AODSIM' # Nov 24 12:27:07 2015
#ds = '/SMS-T1tttt_mGluino-1000_mLSP-1to700_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15FSPremix-MCRUN2_74_V9-v3/AODSIM' #Nov 28 09:36:52 2015
#ds = '/WprimeToMuNu_M-5400_TuneCUETP8M1_13TeV-pythia8/RunIIFall15DR76-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/AODSIM' #Dec  2 09:55:23 2015
#ds = '/WToTauNu_M-1000_TuneCUETP8M1_13TeV-pythia8-tauola/RunIIFall15DR76-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/AODSIM' #Sun Dec  6 04:34:22 2015
#ds = '/TprimeBToBW_M-1400_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIIFall15DR76-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/AODSIM' 
#ds = '/tGamma_FCNC_tGc_13TeV-madgraph-pythia8_TuneCUETP8M1/RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/MINIAODSIM'
ds = '/QCD_Pt_30to50_TuneCUETP8M1_13TeV_pythia8/RunIIFall15DR76-25nsFlat10to50NzshcalRaw_76X_mcRun2_asymptotic_v12-v1/AODSIM'

ds = sys.argv[1]

#around= time.mktime( time.gmtime(session.query(Output).filter(Output.datasetname == ds ).all()[0].date))
outs= []
#for ds in filter(None, open('recent.unlock').read().split('\n'))[:100]:
#    outs.extend(session.query(Output).filter(Output.datasetname == ds).all())
random.shuffle( outs )

outs = session.query(Output).filter(Output.datasetname.contains('Run2015')).filter(Output.datasetname.endswith('DQMIO')).all()
#outs = session.query(Output).filter(Output.date > around - 96*60*60).filter(Output.date < around + 96*60*60).all()




for out in outs:
    if not any([pattern in out.datasetname for pattern in ['ZeroBias/','SinglePhoton/','SingelMuon/','JetHT/']]):
        continue
    #if 'DQM' in out.datasetname: continue
    dataset = out.datasetname
    presence = getDatasetPresence( url, dataset, vetoes=[])
    print dataset
    print presence
    where = [site for site,info in presence.items() if info[0]]
    #print where
    on_tape = (where and any(['MSS' in s for s in where]))
    if on_tape:
        print "on tape"
    else:
        print "\t\t not on tape"
        #nfsvodnf
    if where:
        print "somewhere"
    else:
        print "nowhere"
    continue
#for out in outs:
    rs= listRequests(url, out.datasetname)
    if 'DQM' in out.datasetname: continue
    if not 'AODSIM' in out.datasetname: continue
    print out.datasetname,"inserted by closor at",time.asctime( time.gmtime( out.date))
    all_comments= set()
    all_dest = set()
    for site in rs:
        for phid in rs[site]:
            conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            there = '/phedex/datasvc/json/prod/transferrequests?request=%d'%phid
            r1=conn.request("GET", there)
            r2=conn.getresponse()
            result = json.loads(r2.read())
            #print json.dumps( result['phedex']['request'], indent=2)
            for req in result['phedex']['request']:
                for d in req['destinations']['node']:
                    all_dest.add( d['name'] )
                all_comments.add( req['requested_by']['comments']['$t'])

    has_ddm = any(['IntelROCCS' in com for com in all_comments if com])
    has_tape = any(['MSS' in d for d in all_dest])
    #if all_comments:         print list(all_comments)
    if has_ddm:        print "injected"
    else:        print "\t not injected",out.datasetname
    if has_tape:        print "requested to tape"
    else:        print "\t missing tape request",out.datasetname





"""
before = time.mktime( time.gmtime()) - (7*24*60*60)
outs= session.query(Output).filter(Output.date > before).filter(Output.datasetname.endswith('AODSIM')).all()
random.shuffle( outs )
for output in outs:
    ds = output.datasetname
    print ds
    c,i= findCustodialCompletion(url,  ds) 
    print c
    b,f = findLostBlocksFiles(url,ds)
    if len(b) or len(f):
        print "\t",len(b),"blocks lost"
        print "\t",len(f),"files lost"
"""


#while True:
#    print len(getWorkflowByOutput(url, ds, details=True))


#ds = '/QCD_HT1500to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIIWinter15GS-MCRUN2_71_V1_ext1-v1/GEN-SIM'
#ds = '/TprimeBToBW_M-1400_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIIFall15DR76-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/AODSIM'

#custodials,info = findCustodialCompletion(url, ds)

#print findLateFiles(url, '/MinBias_TuneCUETP8M1_13TeV-pythia8/RunIIWinter15GS-magnetOffBS0T_MCRUN2_71_V1-v1/GEN-SIM' )

#print custodials
#print info

#for ds,info in json.loads(open('lagging_custodial.json').read()).items():
#    for node in info['nodes']:
#        if node in ['T1_UK_RAL_MSS','T1_US_FNAL_MSS']: continue
#        print "check",ds,"at",node
#        findLateFiles(url, ds, going_to = node)

#CI = closeoutInfo()
#CI.assistance()

#for wfo in session.query(Workflow).filter(Workflow.status=='assistance-recovering-filemismatch').all()
#for wfo in session.query(Workflow).filter(Workflow.status=='assistance-manual-filemismatch').all():
#for wfo in session.query(Workflow).filter(Workflow.status=='assistance-recovery-filemismatch').all():
#    wfo.status = wfo.status.replace('-filemismatch','')
#    print wfo.status
#session.commit()

#for wfo in session.query(Workflow).filter(Workflow.status=='close').all():
#    print wfo.name

"""
wfs=[
'vlimant_SUS-RunIISpring15DR74-00095_00507_v0__151127_125505_3753',
'pdmvserv_task_SUS-RunIIFall15DR76-00057__v1_T_160104_165230_8733',
'pdmvserv_HIG-RunIISpring15DR74-00612_00541_v0__151224_002613_3175',
'fabozzi_Run2015C_50ns-HLTPhysicspart1-16Dec2015_763_151218_000906_123',
'fabozzi_Run2015C_50ns-ZeroBias7-16Dec2015_763_151218_001518_734',
'pdmvserv_task_FSQ-RunIIFall15DR76-00008__v1_T_151222_044343_3779',
'fabozzi_Run2015C_50ns-HLTPhysicspart7-16Dec2015_763_151218_001539_8672',
'pdmvserv_task_EXO-RunIIFall15DR76-00248__v1_T_151116_131940_3561',
'pdmvserv_task_HIG-RunIIFall15DR76-00087__v1_T_151121_200140_1766',
'pdmvserv_task_HIG-RunIIFall15DR76-00747__v1_T_151204_210230_1363',
'pdmvserv_task_HIG-RunIIFall15DR76-00187__v1_T_151118_192438_698',
'pdmvserv_task_HIG-RunIIFall15DR76-00261__v1_T_151118_194013_18',
'fabozzi_Run2015C_50ns-Commissioning-16Dec2015_763_151218_001034_4971',
'pdmvserv_task_TOP-RunIIFall15DR76-00011__v1_T_151121_123411_8110',
'pdmvserv_EXO-RunIISpring15MiniAODv2-04086_00320_v0__160104_170603_4393',
'pdmvserv_task_BTV-RunIIFall15DR76-00033__v1_T_151208_202911_8631',
'pdmvserv_task_HIG-RunIIFall15DR76-00076__v1_T_151118_005251_5965',
'pdmvserv_task_TOP-RunIIFall15DR76-00052__v1_T_151209_011352_7084',
'pdmvserv_task_BTV-RunIIFall15DR76-00033__v1_T_151208_202911_8631',
'pdmvserv_task_HIG-RunIIFall15DR76-00163__v1_T_151121_200050_6052 ',
'pdmvserv_task_TOP-RunIIFall15DR76-00052__v1_T_151209_011352_7084',
'pdmvserv_task_BTV-RunIIFall15DR76-00016__v1_T_151117_230658_8314',
'pdmvserv_BTV-RunIIFall15DR76-00074_00194_v0__151218_202539_7708',
'pdmvserv_BTV-RunIIFall15DR76-00042_00265_v0__160103_233318_5225',
'pdmvserv_task_HIG-RunIIFall15DR76-00938__v1_T_160106_010239_7621',
'pdmvserv_BTV-RunIIFall15DR76-00045_00265_v0__160103_233346_8062',
'pdmvserv_task_TOP-RunIIFall15DR76-00050__v1_T_151206_215154_9991',
'pdmvserv_task_TOP-RunIIFall15DR76-00050__v1_T_151206_215154_9991',
'pdmvserv_task_HIG-RunIIFall15DR76-00803__v1_T_151209_042949_2779',

]
for wf in wfs:
    for wfo in session.query(Workflow).filter(Workflow.name == wf).all():
        print wfo.status,wfo.status
        wfo.status = 'staging'
session.commit()
"""

"""
#
ds='/GluGluHToGG_M130_13TeV_amcatnloFXFX_pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM'
ds='/BprimeTToZB_M-1100_RH_TuneCUETP8M1_13TeV-madgraph-pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM'
ds='/ZH_HToGG_ZToAll_M120_13TeV_powheg_pythia8/RunIISummer15GS-MCRUN2_71_V1-v1/GEN-SIM'
ds= sys.argv[1]

#a=json.loads(open('lost_blocks_datasets.json.saved').read())
a = [ ds ]
aa={}
for ds in a:
    b,f = findLostBlocksFiles(url,ds)
    if b:
        print 'blocks',[i['name'] for i in b]
        aa[ds] = [i['name'] for i in b]
    if f:print 'files',[i['name'] for i in f]
    print getDatasetBlockFraction(ds,[i['name'] for i in b])
    print getDatasetFileFraction( ds,[i['name'] for i in f])

print json.dumps( aa, indent=2)
"""

"""
for dsname in json.loads(open('lost_blocks_datasets.json').read()):
    lost,files = findLostBlocksFiles(url, dsname)
    lost_names = [item['name'] for item in lost]
    print dsname
    print getDatasetBlockFraction( dsname, lost_names )
"""

#for wfo in session.query(Workflow).filter(Workflow.status == 'staging').filter(Workflow.name.contains('2015C_25')).all():
#    print wfo.name,wfo.status
#    wfo.status = 'considered'  
#session.commit()




#print "\n".join(d)

#data = json.loads(open('/afs/cern.ch/user/c/cmst2/www/unified/incomplete_transfers.json').read())
#for dataset in data:
#    for phedexid in data[dataset]:
#        print dataset,phedexid


#for wfo in session.query(Workflow).filter(Workflow.status =='away').filter(Workflow.name.startswith('vlimant')).all():
#    print wfo.name


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
