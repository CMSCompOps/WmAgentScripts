#!/usr/bin/env python
import optparse, json, time
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import getRelValDsetNames
#import pprint


reqmgr_url = 'cmsweb.cern.ch'
dbs3_url = 'https://cmsweb.cern.ch'

def getRequestJson(workflow):
    """
    Fetches the request spec file from ReqMgr
    """
    conn = httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+workflow)
    r2=conn.getresponse()
    request = json.loads(r2.read())
    return request

def getRequestStatus(workflow):
    conn  =  httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
    r2=conn.getresponse()
    s = json.loads(r2.read())
    return s['RequestStatus']

def setRequestStatus(workflow):
    conn  =  httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    headers={"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    params = {"requestName" : workflow, "cascade" : True}
    encodedParams = urllib.urlencode(params)
    conn.request("POST", "/reqmgr/reqMgr/closeout", encodedParams, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()

def getEventsDataSetRunList(dbsApi, dset, runList, verb = False):
    """
    Provided a dataset and a list of runs, it gets the num of
    events per run inside the dataset and sum up all of them
    Returns the total num of events or -10 if datatier not allowed
    """
    # we cannot calculate completion of ALCARECO, DQMIO and DQMROOT samples
    if '/ALCARECO' in dset or  '/DQMIO' in dset or '/DQMROOT' in dset:
        return -10

    total = 0
    # get run by run, so URI wont be too large
    for run in runList:
        reply = dbsApi.listFileSummaries(dataset = dset,run_num = run)
        if reply:
            #print reply[0]['num_event']
            total += reply[0]['num_event']

    return total

def getOutputEvents(dbsApi, dset, verb = False):
    """
    Get the num of events in the output dataset provided it
    has a valid data tier.
    Returns the num of events or -10 if datatier not allowed
    """
    # we cannot calculate completion of ALCARECO, DQMIO and DQMROOT samples
    if '/ALCARECO' in dset or  '/DQMIO' in dset or '/DQMROOT' in dset:
        return -10

    outputEvents = getRelValDsetNames.getNumEvents(dbsApi, dset)

    return outputEvents

def main():
    parser = optparse.OptionParser()
    parser.add_option('--test',action="store_true", help='Nothing is closed out. Only test if the workflows are ready to be closed out.',dest='test')
    parser.add_option('--verbose',action="store_true", help='Print out details about the number of events expected and produced.',dest='verbose')
    parser.add_option('--correct_env',action="store_true",dest='correct_env')
    (options,args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if not options.correct_env:
        os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; our\
ce /tmp/relval/sw/comp.pre/slc6_amd64_gcc481/cms/dbs3-client/3.2.8a/etc/profile.d/init.sh; python2.6 "+command + "--correct_env")
        sys.exit(0)
            

    if len(args) != 1:
        print "Usage:"
        print "python closeOutTaskChainWorkflows.py [--test] [--verbose] <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)

    inputFile=args[0]
    f = open(inputFile, 'r')

    closedOut = []
    nonClosedOut = []
    tooManyEventsOrLumis = []
    running = []

    dbsApi = getRelValDsetNames.getDBSApi()

    for line in f:
        workflow = line.rstrip('\n')
        if options.verbose:
            print "checking workflow " + workflow
        schema = getRequestJson(workflow)
        if schema['RequestType'] != 'TaskChain':
            print "workflow type is not TaskChain, exiting"
            sys.exit(0)

        outputDatasets = getRelValDsetNames.getOutputDset(workflow)
#        pprint.pprint(outputDatasets)
        # We should never hit this case
        if 'RequestNumEvents' in schema['Task1'] and 'InputDataset' in schema['Task1']:
            print "Request cannot have both RequestNumEvents and InputDataset in Task1, exiting..."
            sys.exit(1)

        # Check whether it's FastSim or FullSim from scratch
        if 'RequestNumEvents' in schema['Task1']:
            inputEvents = schema['Task1']['RequestNumEvents']
#            if options.verbose:
#                print "DEBUG: RequestNumEvents: %d" % inputEvents

            closeOut = True
            tooMany = False
            for dataset in outputDatasets:
                outputEvents = 0

                outputEvents = getOutputEvents(dbsApi, dataset, verb = options.verbose)
                if options.verbose:
                    successRate = outputEvents/float(inputEvents)
                    print "  %-110s\t%d\t%.1f%%" % (dataset, outputEvents, successRate*100)

                if outputEvents == -10:
                    continue
                elif outputEvents == inputEvents:
                    pass
                elif outputEvents < inputEvents :
                    closeOut = False
                elif outputEvents > inputEvents :
                    closeOut = False
                    tooMany = True

            if closeOut:
                closedOut.append(workflow)
            else:
                nonClosedOut.append(workflow)
            if tooMany:
                tooManyEventsOrLumis.append(workflow)

        # Then it's either Data or MC recycling
        elif 'InputDataset' in schema['Task1']:
            inputDset = schema['Task1']['InputDataset']

            # It's Data
            if 'RunWhitelist' in schema['Task1']:
                closeOut = True
                tooMany = False

                runList = schema['Task1']['RunWhitelist']
#                if options.verbose:
#                    print "DEBUG: InputDset %s and runList is %r" % (inputDset, runList)

                inputEvents = getEventsDataSetRunList(dbsApi, inputDset, runList, verb = options.verbose)
#                if options.verbose:
#                    print "DEBUG: InputDset %s and %d events" % (inputDset, inputEvents)

                for dataset in outputDatasets:
                    outputEvents = 0

                    outputEvents = getOutputEvents(dbsApi, dataset, verb = options.verbose)
                    if options.verbose:
                        successRate = outputEvents/float(inputEvents)
                        print "  %-110s\t%d\t%.1f%%" % (dataset, outputEvents, successRate*100)

                    if outputEvents == -10:
                        continue
                    elif outputEvents == inputEvents:
                        pass
                    elif outputEvents < inputEvents :
                        closeOut = False
                    elif outputEvents > inputEvents :
                        #print outputEvents
                        #print inputEvents
                        closeOut = False
                        tooMany = True

                if closeOut:
                    closedOut.append(workflow)
                else:
                    nonClosedOut.append(workflow)
                if tooMany:
                    tooManyEventsOrLumis.append(workflow)

            elif 'BlockWhitelist' in schema['Task1']:
                print "TODO: you need to code me to handle block white list"
                nonClosedOut.append(workflow)
                break

            # Most likely MC, since there is no run whitelist
            # it means we can just go for num of events
            else:
                closeOut = True
                tooMany = False

                inputEvents = getRelValDsetNames.getNumEvents(dbsApi, inputDset)
#                if options.verbose:
#                    print "DEBUG: InputDset %s and %d events" % (inputDset, inputEvents)

                for dataset in outputDatasets:
                    outputEvents = 0

                    outputEvents = getOutputEvents(dbsApi, dataset, verb = options.verbose)
                    if options.verbose:
                        successRate = outputEvents/float(inputEvents)
                        print "  %-110s\t%d\t%.1f%%" % (dataset, outputEvents, successRate*100)
                
                    if outputEvents == -10:
                        continue
                    elif outputEvents == inputEvents:
                        pass
                    elif outputEvents < inputEvents :
                        closeOut = False
                    elif outputEvents > inputEvents :
                        closeOut = False
                        tooMany = True

                if closeOut:
                    closedOut.append(workflow)
                else:
                    nonClosedOut.append(workflow)
                if tooMany:
                    tooManyEventsOrLumis.append(workflow)

    print '-----------------------------------------------------------------------------------------------------------------------------------------------'
    print '| Request                                                                                             | Closed-out? | Current status          |'
    print '-----------------------------------------------------------------------------------------------------------------------------------------------'

    for workflow in closedOut:
        status = getRequestStatus(workflow)
        if status == 'completed' and not options.test:
            setRequestStatus(workflow)
            status = 'closed-out'
        else:
            pass
        print "%100s\tYES\t\t%s" % (workflow, status)

    for workflow in nonClosedOut:
        status = getRequestStatus(workflow)
        print "%100s\tNO\t\t%s" % (workflow, status)

    print '-----------------------------------------------------------------------------------------------------------------------------------------------'

    for workflow in tooManyEventsOrLumis:
        print "WARNING (more lumis and/or events --> " + workflow
    f.close
    sys.exit(0)

if __name__ == "__main__":
    main()
