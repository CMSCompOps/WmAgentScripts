#!/usr/bin/env python
#import json
import sys
import json
import reqMgrClient as reqmgr
import optparse

def setStatus(url, workflowname,newstatus):
    print "Setting %s to %s" % (workflowname,newstatus)
    if newstatus == 'closed-out':
        return reqmgr.closeOutWorkflowCascade(url, workflowname)
    elif newstatus == 'announced':
        return reqmgr.announceWorkflowCascade(url, workflowname)
    else:
        print "cannot cascade to",newstatus

def getStatus(url, workflow):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
    r2=conn.getresponse()
    #data = r2.read()
    s = json.loads(r2.read())
    t = s['RequestStatus']
    return t

def main():
    
    parser = optparse.OptionParser()
    parser.add_option('-u','--url',help='Which server to communicate with', default='cmsweb.cern.ch',choices=['cmsweb.cern.ch','cmsweb-testbed.cern.ch'])
    parser.add_option('-w','--wf',help='Filelis of coma separated list of workflows')
    parser.add_option('-s','--status',help='The new status', choices=['closed-out','announced'])
    (options,args) = parser.parse_args()

    wfs = []
    try:
        f = open(options.wf, 'r')
        wfs.extend([l.strip('\n').strip(' ') for l in f])
        f.close()
    except:
        wfs.extend(options.wf.split(','))
    
    for wf in wfs:
        r = setStatus(options.url, wf, options.status)


if __name__ == "__main__":
    main()
