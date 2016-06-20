#if two workflows write into the same dataset, this dataset will have more events than expected

#!/usr/bin/env python
import optparse, json, time
import urllib2,urllib, httplib, sys, re, os
from xml.dom.minidom import getDOMImplementation
import collect_dsets_and_nevents

reqmgr_url = 'cmsweb.cern.ch'
dbs3_url = 'https://cmsweb.cern.ch'

def getRequestJson(workflow):
    """
    Fetches the request spec file from ReqMgr
    """
    headers = {"Content-type": "application/json", "Accept": "application/json"}

    conn = httplib.HTTPSConnection(reqmgr_url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

    r1=conn.request('GET','/reqmgr2/data/request/' + workflow, headers=headers)
    r2=conn.getresponse()
    request = json.loads(r2.read())

    if ('result' not in request) or (len(request['result']) != 1) or (workflow not in request['result'][0]):
        os.system('echo \"'+request +'\" | mail -s \"too_many_events_check.py error 3\" andrew.m.levin@vanderbilt.edu')
        sys.exit(1)

    return request['result'][0][workflow]

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

    outputEvents = collect_dsets_and_nevents.getNumEvents(dbsApi, dset)

    return outputEvents

def too_many_events_check(wf_name):

    schema = getRequestJson(wf_name)

    #this check only works for taskchain workflows
    if schema['RequestType'] != 'TaskChain':
        return

    dbsApi = collect_dsets_and_nevents.getDBSApi()

    outputDatasets = collect_dsets_and_nevents.getOutputDset(wf_name)

    # We should never hit this case
    if 'RequestNumEvents' in schema['Task1'] and 'InputDataset' in schema['Task1']:
        os.system('echo \"'+wf_name +'\" | mail -s \"too_many_events_check.py error 1\" andrew.m.levin@vanderbilt.edu')
        sys.exit(1)

    # Check whether it's FastSim or FullSim from scratch
    if 'RequestNumEvents' in schema['Task1']:
        inputEvents = schema['Task1']['RequestNumEvents']
        for dataset in outputDatasets:
            outputEvents = 0

            outputEvents = getOutputEvents(dbsApi, dataset, verb = False)
            
            if outputEvents > inputEvents :
                os.system('echo '+wf_name+' | mail -s \"too_many_events_check.py error 2\" andrew.m.levin@vanderbilt.edu')
                sys.exit(1)


    # Then it's either Data or MC recycling
    elif 'InputDataset' in schema['Task1']:
        inputDset = schema['Task1']['InputDataset']

            # It's Data
        if 'RunWhitelist' in schema['Task1']:

            runList = schema['Task1']['RunWhitelist']

            inputEvents = getEventsDataSetRunList(dbsApi, inputDset, runList, verb = False)

            for dataset in outputDatasets:
                outputEvents = 0

                outputEvents = getOutputEvents(dbsApi, dataset, verb = False)
                if outputEvents > inputEvents :
                    os.system('echo '+wf_name+' | mail -s \"too_many_events_check.py error 3\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(1)

        elif 'BlockWhitelist' in schema['Task1']:
            os.system('echo '+wf_name+' | mail -s \"too_many_events_check error.py 4\" andrew.m.levin@vanderbilt.edu')
            sys.exit(1)

            # Most likely MC, since there is no run whitelist
            # it means we can just go for num of events
        else:
            inputEvents = collect_dsets_and_nevents.getNumEvents(dbsApi, inputDset)

            for dataset in outputDatasets:
                outputEvents = 0

                outputEvents = getOutputEvents(dbsApi, dataset)
                if outputEvents > inputEvents :
                    os.system('echo '+wf_name+' | mail -s \"too_many_events_check.py error 5\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(1)

