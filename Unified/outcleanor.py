#!/usr/bin/env python
from assignSession import *
from utils import workflowInfo, getDatasetPresence, getDatasetStatus, getWorkflowByInput, getDatasetSize, makeDeleteRequest, listDelete, approveSubscription, findCustodialLocation
from utils import lockInfo, duplicateLock
import optparse
import random 
from collections import defaultdict
import json 
import time

def outcleanor(url, options):
    print "Deprecated"
    return 

    if duplicateLock(): return 

    do_not_autoapprove = []#'T2_FR_CCIN2P3']
    LI = lockInfo()


    sites_and_datasets = defaultdict(list)
    our_copies = defaultdict(list)
    wf_cleaned = {}
    
    wfs = []
    for fetch in options.fetch.split(','):
        wfs.extend(session.query(Workflow).filter(Workflow.status==fetch).all())

    random.shuffle( wfs )
    last_answer = None
    for wfo in wfs :
        if options.number and len(wf_cleaned)>= options.number:
            print "Reached",options.number,"cleaned"
            break
        print '-'*100
        wfi = workflowInfo(url, wfo.name)
        goes = {} # boolean per output
        for dataset in wfi.request['OutputDatasets']:
            goes[dataset] = False
            keep_one_out = False ## change to no copy kept, since this is DDM handled
            status = getDatasetStatus( dataset )
            print "\n\tLooking at",dataset,status,"\n"
            vetoes = None
            if status == 'INVALID':
                vetoes = ['Export','Buffer'] ## can take themselves out
                keep_one_out = False # just wipe clean

            elif status == None:
                print dataset,"actually does not exist. skip"
                goes[dataset] = True
                continue

            elif status in ['PRODUCTION','VALID'] and wfo.status in ['forget','trouble']:
                print dataset,"should probably be invalidated. (",wfo.status,") skip"
                keep_one_out = False # just wipe clean
                continue ## you are not sure. just skip it for the time being

            elif status == 'PRODUCTION' and wfo.status in ['clean']:
                print dataset,"should probably be set valid .skip"
                continue ## you are not sure. just skip it for the time being

            if status == 'VALID' and dataset.startswith('/MinBias'):
                print "This is a /MinBias. skip"
                continue

            if '/DQM' in dataset:
                keep_one_out = False

            custodials = findCustodialLocation(url, dataset)
            if not len(custodials):
                print dataset,"has no custodial site yet, excluding from cleaning"
                continue

            total_size = getDatasetSize( dataset )
            
            our_presence = getDatasetPresence(url, dataset, complete=None, group="DataOps", vetoes=vetoes)
            also_our_presence = getDatasetPresence(url, dataset, complete=None, group="", vetoes=vetoes)
            
            ## merge in one unique dict
            for site in also_our_presence:
                if site in our_presence:
                    there,frac = our_presence[site]
                    other,ofrac = also_our_presence[site]
                    our_presence[site] = (max(there,other),max(frac,ofrac))
                else:
                    our_presence[site] = also_our_presence[site]
                
            if our_presence: print our_presence

            ## analysis ops copies need to be taken into account
            anaops_presence = getDatasetPresence(url, dataset, complete=None, group="AnalysisOps")
            own_by_anaops = anaops_presence.keys()
            
            ## all our copies
            to_be_cleaned = our_presence.keys()
            if not len(to_be_cleaned):
                print "nowhere to be found of ours,",len(own_by_anaops),"in analysi ops pool"
                goes[dataset] = True
                continue

            print "Where we own bits of dataset"
            print to_be_cleaned
     

            if len(own_by_anaops):
                ## remove site with the anaops copies
                to_be_cleaned = list(set(to_be_cleaned) - set(own_by_anaops))
                keep_one_out = False ## in that case, just remove our copies
                print "Own by anaops (therefore not keep a copy of ours)"
                print own_by_anaops
            else:
                ## we should not be looking at anything that was not passed to DDM, otherwise we'll be cutting the grass under our feet
                using_the_same = getWorkflowByInput(url, dataset, details=True)
                conflict = False
                for other in using_the_same:
                    if other['RequestName'] == wfo.name: continue
                    if other['RequestType'] == 'Resubmission': continue
                    if not other['RequestStatus'] in ['announced','normal-archived','aborted','rejected','aborted-archived','rejected-archived','closed-out','None',None]:
                        print other['RequestName'],'is in status',other['RequestStatus'],'preventing from cleaning',dataset
                        conflict=True
                        break
                if conflict:
                    continue

                ## not being used. a bit less dangerous to clean-out
                ## keep one full copy out there
                full_copies = [site for (site,(there,fract)) in our_presence.items() if there]
                if keep_one_out:
                    if not len(full_copies):
                        print "we do not own a full copy of",dataset,status,wfo.status,".skip"
                        continue
                    t1_full_copies = [ site for site in full_copies if site.startswith('T1')]
                    if t1_full_copies:
                        stay_there = random.choice( t1_full_copies ) #at a place own by ops
                    else:
                        stay_there = random.choice( full_copies ) #at a place own by ops
                    print "Where we keep a full copy", stay_there
                    to_be_cleaned.remove( stay_there )
                    our_copies[stay_there].append( dataset )
                    LI.release_except( dataset, stay_there, 'cleanup of output after production')            
                else:
                    print "We do not want to keep a copy of ",dataset,status,wfo.status
                    LI.release_everywhere( dataset, 'cleanup of output after production')

            if len(to_be_cleaned):
                print "Where we can clean"
                print to_be_cleaned
                for site in to_be_cleaned:
                    sites_and_datasets[site].append( (dataset, total_size*our_presence[site][1]/100., status) )
                goes[dataset] = True
            else:
                print "no cleaning to be done"
                goes[dataset] = True

        print wfo.name,"scrutinized"
        if all(goes.values()):
            print "\t",wfo.name,"can toggle -out"
        def ask():
            global last_answer
            last_answer = raw_input('go on ?')
            return last_answer
        if options.auto or ask() in ['y','']:
            if all(goes.values()):
                wfo.status = wfo.status+'-out'
                wf_cleaned[wfo.name] = wfo.status
            continue
        elif last_answer in ['q','n']:
            break
        else:
            return 

    if options.auto:
        pass
    elif last_answer in ['q']:
        return

    print "Potential cleanups"
    for (site,items) in sites_and_datasets.items():
        cleanup = sum([size for (_,size,_) in items])
        print "\n\t potential cleanup of","%8.4f"%cleanup,"GB at ",site
        print "\n".join([ds+" "+st for ds,_,st in items])
        datasets = [ ds for ds,_,st in items]

    print "Copies and bits we are going to delete"
    print json.dumps( sites_and_datasets, indent=2)

    print "Copies we are keeping"
    print json.dumps( our_copies, indent=2 )     

    print "Workflows cleaned for output"
    print json.dumps( wf_cleaned, indent=2 )
    #stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
    #open('outcleaning_%s.json'%stamp,'w').write( json.dumps( sites_and_datasets, indent=2))
    #open('keepcopies_%s.json'%stamp,'w').write( json.dumps( our_copies, indent=2))
    #open('wfcleanout_%s.json'%stamp,'w').write( json.dumps( wf_cleaned, indent=2))


    if (not options.test) and (options.auto or raw_input("Satisfied ? (y will trigger status change and deletion requests)") in ['y']):
        for (site,items) in sites_and_datasets.items():
            #datasets = [ ds for ds,_,st in items]
            #is_tape = any([v in site for v in ['MSS','Export','Buffer'] ])
            #comments="Cleanup output after production. DataOps will take care of approving it."
            #if is_tape:
            #    comments="Cleanup output after production."
            #print "making deletion to",site
            #result = makeDeleteRequest(url, site, datasets, comments=comments)
            #print result
            ## approve it right away ?
            #for did in [item['id'] for item in result['phedex']['request_created']]:
            #    if not is_tape:
            #        print "auto-approving to",site,"?"
            #        if not site in do_not_autoapprove:
            #            approveSubscription(url, did, nodes = [site], comments = 'Production cleaning by data ops, auto-approved')
            #        pass
            pass
        session.commit()
    else:
        print "Not making the deletion and changing statuses"

    
if __name__ == "__main__":
    url = 'cmsweb.cern.ch'

    parser = optparse.OptionParser()
    parser.add_option('-t','--test', help='Only test the cleanout',action='store_true',default=False)
    parser.add_option('-n','--number',help='Specify the amount of wf to clean',type=int, default=0)
    parser.add_option('-a','--auto',help='Do not ask confirmation',action='store_true',default=False)
    parser.add_option('-f','--fetch',help='The coma separated list of status to fetch from',default='clean,forget')
    (options,args) = parser.parse_args()
    
    outcleanor(url, options)
