"""
__DBS.checkRequest__

Created on Apr 11, 2013

@author: dballest
"""

import sys
import json
import time

from WMCore.Services.Requests import JSONRequests
from WMCore.Services.DBS.DBSReader import DBSReader

def getRequestInfo():
    requestName = sys.argv[1]
    print "Getting information from cmsweb about %s... %s" % (requestName, time.strftime('%H:%M:%S'))
    requestor = JSONRequests(url = 'https://cmsweb.cern.ch/reqmgr/reqMgr')
    response = requestor.get('/request?requestName=%s' % requestName)
    if response[1] != 200:
        raise RuntimeError("Request information was not available!")
    requestDict = response[0]
    compactRequest = {'InputDataset' : requestDict['InputDataset'],
                      'RunWhitelist' : requestDict['RunWhitelist'],
                      'RunBlacklist' : requestDict['RunBlacklist'],
                      'BlockWhitelist' : requestDict['BlockWhitelist'],
                      'BlockBlacklist' : requestDict['BlockBlacklist'],
                      'OutputDatasets' : []}
    response = requestor.get('/outputDatasetsByRequestName?requestName=%s' % requestName)
    if response[1] != 200:
        raise RuntimeError("Output dataset information was not available!")
    compactRequest['OutputDatasets'] = response[0]
    print "Done querying cmsweb... %s" % time.strftime('%H:%M:%S')
    return compactRequest

def getDBSSummary(requestInfo):
    print "Loading DBS full information for %s... %s" % (requestInfo['InputDataset'], time.strftime('%H:%M:%S'))
    reader = DBSReader('http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')
    files = reader.listDatasetFileDetails(datasetPath = requestInfo['InputDataset'])
    inputRunAndLumis = {}
    for lfn in files.keys():
        fileInfo = files[lfn]
        runAndLumis = fileInfo['Lumis']
        block = fileInfo['BlockName']
        if requestInfo['BlockWhitelist'] and block not in requestInfo['BlockWhitelist']:
            del files[lfn]
            continue
        if requestInfo['BlockBlacklist'] and block in requestInfo['BlockBlacklist']:
            del files[lfn]
            continue
        for run in runAndLumis.keys():
            if requestInfo['RunWhitelist'] and run not in requestInfo['RunWhitelist']:
                del runAndLumis[run]
                continue
            if requestInfo['RunBlacklist'] and run not in requestInfo['RunBlacklist']:
                del runAndLumis[run]
                continue
            if run not in inputRunAndLumis:
                inputRunAndLumis[run] = set()
            inputRunAndLumis[run].update(set(runAndLumis[run]))
        if not runAndLumis:
            del files[lfn]
            continue
    outputRunAndLumis = {}
    for outputDataset in requestInfo['OutputDatasets']:
        print "Loading DBS full information for %s... %s" % (outputDataset, time.strftime('%H:%M:%S'))
        outputRunAndLumis[outputDataset] = {}
        outputFiles = reader.listDatasetFileDetails(datasetPath = outputDataset)
        for lfn in outputFiles:
            fileInfo = outputFiles[lfn]
            runAndLumis = fileInfo['Lumis']
            for run in runAndLumis:
                if run not in outputRunAndLumis[outputDataset]:
                    outputRunAndLumis[outputDataset][run] = set()
                outputRunAndLumis[outputDataset][run].update(set(runAndLumis[run]))
    differences = {}
    for outputDataset in outputRunAndLumis:
        print "Analyzing differences in %s... %s" % (outputDataset, time.strftime('%H:%M:%S'))
        differences[outputDataset] = {}
        for run in inputRunAndLumis:
            diff = inputRunAndLumis[run] - outputRunAndLumis[outputDataset].get(run, set())
            if diff:
                differences[outputDataset][run] = diff
        jsonizedMissingLumis = {}
        for run in differences[outputDataset]:
            interval = []
            jsonizedMissingLumis[run] = []
            for lumi in sorted(differences[outputDataset][run]):
                if not interval:
                    interval = [lumi,lumi]
                elif lumi == interval[1] + 1:
                    interval[1] = lumi
                else:
                    jsonizedMissingLumis[run].append(interval)
                    interval = [lumi,lumi]
            if interval:
                jsonizedMissingLumis[run].append(interval)
        try:
            if not jsonizedMissingLumis:
                continue
            outFileName = 'MissingLumis_%s.json' % outputDataset.replace('/', '_')
            outFileHandle = open(outFileName , 'w')
            json.dump(jsonizedMissingLumis, outFileHandle)
            outFileHandle.close()
        except:
            print "Error writing to %s" % outFileName


def main():
    print "Starting... %s" % time.strftime('%H:%M:%S')
    requestInfo =  getRequestInfo()
    getDBSSummary(requestInfo)

if __name__ == '__main__':
    sys.exit(main())