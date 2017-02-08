import reqMgrClient
import httplib
import json
import os
import sys


def make_assignment_params(schema,site, processing_version):
    procstring = {}
    
    maxrss = {}

    if "CMSSWVersion" not in schema:
        os.system('echo assignment.py error 7 | mail -s \"assignment.py error 7\" andrew.m.levin@vanderbilt.edu')        
        print "CMSSWVersion not in schema"
        sys.exit(1)
    
    acqera = schema['CMSSWVersion']

    #use this as the default maxrss
    maxrss_main = 3072000

    for key, value in schema.items():
        if key == "ProcessingString":
            procstring_main = value.replace('-','_')
            if '-' in value:
                os.system('echo '+schema['RequestName']+' | mail -s \"assignment.py warning 2\" andrew.m.levin@vanderbilt.edu')

    for key, value in schema.items():
        if key == "Memory":
            maxrss_main = int(value)*1024

    if schema['RequestType'] == 'TaskChain':
        for key, value in schema.items():
            if type(value) is dict and key.startswith("Task"):
                if 'ProcessingString' in value:
                    procstring[value['TaskName']] = value['ProcessingString'].replace("-","_")
                    if "-" in value['ProcessingString']:
                        os.system('echo '+schema['RequestName']+' | mail -s \"assignment.py warning 1\" andrew.m.levin@vanderbilt.edu')
                elif "procstring_main" in vars():
                    procstring[value['TaskName']] = procstring_main
                else:
                    os.system('echo '+schema['RequestName']+' | mail -s \"assignment.py error 6\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)    
                if 'Memory' in value:
                    maxrss[value['TaskName']] = int(value['Memory'])*1024
                else:
                    maxrss[value['TaskName']] = maxrss_main
                if 'AcquisitionEra' in value:
                    if '-' in value['AcquisitionEra']:
                        os.system('echo '+schema['RequestName']+' | mail -s \"assignment.py error 3\" andrew.m.levin@vanderbilt.edu')
                        sys.exit(1)
    else:
        procstring = procstring_main
        maxrss = maxrss_main

    params = {
        #                'SiteWhitelist' : ["T2_CH_CERN","T2_CH_CERN_T0"],
        'SiteWhitelist' : site,
        'SiteBlacklist' : [],
        'MergedLFNBase' : '/store/relval',
        'TrustSitelists' : True,
        'CustodialSites' : [], 
        'ProcessingVersion' : int(processing_version),
        'ProcessingString' : procstring,
        'Dashboard': 'relval',
#        'dashboard': 'relval',
        'AcquisitionEra': acqera,
        'BlockCloseMaxWaitTime' : 28800,
        "SoftTimeout" : 129600,
        "MaxRSS" : maxrss,
        "MaxVSize": 4394967000
        }

    params['execute'] = True

    return params

if __name__ == "__main__":

    if len(sys.argv) != 5 and len(sys.argv) != 4:
        print "Usage"
        print "assignment.py wf_name site processing_version [processing_string]"
        sys.exit(0)

    headers = {"Content-type": "application/json", "Accept": "application/json"}

    conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))    
    r1=conn.request("GET",'/reqmgr2/data/request/'+sys.argv[1], headers = headers)
    r2=conn.getresponse()
    schema = json.loads(r2.read())

    schema = schema['result']
    
    if len(schema) != 1:
        os.system('echo '+sys.argv[1]+' | mail -s \"assignment.py error 8\" andrew.m.levin@vanderbilt.edu')
        sys.exit(1)

    schema = schema[0]

    schema = schema[sys.argv[1]]

    print '/reqmgr/reqMgr/request?requestName='+sys.argv[1]

    params = make_assignment_params(schema,sys.argv[2],sys.argv[3])

    if len(sys.argv) == 5:
        params["ProcessingString"] = sys.argv[4]

    reqMgrClient.assignWorkflow("cmsweb.cern.ch", sys.argv[1], "relval", params)
