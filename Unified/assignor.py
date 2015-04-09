from assignSession import *
import reqMgrClient
from utils import workflowInfo, campaignInfo, siteInfo
from utils import getSiteWhiteList, getWorkLoad, getDatasetPresence, getDatasets, findCustodialLocation
import optparse
import itertools
import time

def assignor(url ,specific = None, talk=True, options=None):
    CI = campaignInfo()
    SI = siteInfo()
    wfos=[]
    if specific:
        wfos = session.query(Workflow).filter(Workflow.name==specific).all()
    if not wfos:
        if specific:
            wfos = session.query(Workflow).filter(Workflow.status=='considered').all()
            wfos.extend( session.query(Workflow).filter(Workflow.status=='staging').all())
        wfos.extend(session.query(Workflow).filter(Workflow.status=='staged').all())

    for wfo in wfos:
        if specific:
            if not any(map(lambda sp: sp in wfo.name,specific.split(','))): continue
            #if not specific in wfo.name: continue
        print wfo.name,"to be assigned"
        wfh = workflowInfo( url, wfo.name)
        #wl = getWorkLoad(url, wfo.name )

        if not CI.go( wfh.request['Campaign'] ):
            print "No go for",wfh.request['Campaign']
            continue

        injection_time = time.mktime(time.strptime('.'.join(map(str,wfh.request['RequestDate'])),"%Y.%m.%d.%H.%M.%S")) / (60.*60.)
        now = time.mktime(time.gmtime()) / (60.*60.)
        if float(now - injection_time) < 4.:
            print "It is too soon to inject: %3.2fH remaining"%(now - injection_time)
            if not options.test:
                continue

        #grace_period = 4 #days
        #if float(now - injection_time) > grace_period*24.:
        #    print "it has been",grace_period,"need to do something"
        #    options.restrict = True

        #else:
        #    print now,injection_time,now - injection_time

        #print wl
        if wfh.request['RequestStatus'] !='assignment-approved':
            print wfo.name,wfh.request['RequestStatus'],"skipping"
            if not options.test:
                continue

        version=wfh.getNextVersion()

        (lheinput,primary,parent,secondary) = wfh.getIO()
        sites_allowed = getSiteWhiteList( (lheinput,primary,parent,secondary) )
        sites_custodial = list(set(itertools.chain.from_iterable([findCustodialLocation(url, prim) for prim in primary])))
        sites_out = [SI.pick_dSE([SI.CE_to_SE(ce) for ce in sites_allowed])]
        if len(sites_custodial)==0:
            sites_custodial = [SI.pick_SE()]
            print "picked",sites_custodial," as custodial for",wfo.name

        if len(sites_custodial)>1:
            print "more than one custodial for",wfo.name
            sys.exit(36)

        if options.restrict:
            if talk:
                print sites_allowed
            ## restrict to where the primary and secondary are
            for prim in list(primary)+list(secondary):
                presence = getDatasetPresence( url, prim )
                if talk:
                    print prim,presence
                sites_allowed = [site for site in sites_allowed if any([osite.startswith(site) for osite in [psite for (psite,frac) in presence.items() if frac[1]>90.]])]
            sites_allowed=list(set(sites_allowed))

        if not len(sites_allowed):
            print wfo.name,"cannot be assign with no matched sites"
            continue

        parameters={
            'SiteWhitelist' : sites_allowed,
            'CustodialSites' : sites_custodial,
            'NonCustodialSites' : sites_out,
            'AutoApproveSubscriptionSites' : list(set(sites_out+sites_custodial)),
            'AcquisitionEra' : wfh.acquisitionEra(),
            'ProcessingString' : wfh.processingString(),
            'MergedLFNBase' : '/store/mc', ## to be figured out ! from Hi shit
            'ProcessingVersion' : version,
            }

        ##parse options entered in command line if any
        if options:
            for key in reqMgrClient.assignWorkflow.keys:
                v=getattr(options,key)
                if v!=None:
                    if ',' in v: parameters[key] = filter(None,v.split(','))
                    else: parameters[key] = v

        ## take care of a few exceptions
        if (wfh.request['Memory']*1000) > 3000000:
            parameters['MaxRSS'] = 4000000

        ## pick up campaign specific assignment parameters
        parameters.update( CI.parameters(wfh.request['Campaign']) )

        if not options.test:
            parameters['execute'] = True

        if not wfh.checkWorkflowSplitting():
            ## needs to go to event based ? fail for now
            print "Falling back to event splitting ?"
            parameters['SplittingAlgorithm'] = 'EventBased'

        ## plain assignment here
        result = reqMgrClient.assignWorkflow(url, wfo.name, 'production', parameters)

        # set status
        if not options.test:
            if result:
                wfo.status = 'away'
                session.commit()
            else:
                print "ERROR could not assign",wfo.name
        else:
            pass

    
if __name__=="__main__":
    url = 'cmsweb.cern.ch'

    parser = optparse.OptionParser()
    #parser.add_option('-e', '--execute', help='Actually assign workflows',action="store_true",dest='execute')
    parser.add_option('-t','--test', help='Only test the assignment',action='store_true',dest='test',default=False)
    parser.add_option('-r', '--restrict', help='Only assign workflows for site with input',default=False, action="store_true",dest='restrict')

    for key in reqMgrClient.assignWorkflow.keys:
        parser.add_option('--%s'%key,help="%s Parameter of request manager assignment interface"%key, default=None)
    (options,args) = parser.parse_args()

    spec=None
    if len(args)!=0:
        spec = args[0]

    assignor(url,spec, options=options)
