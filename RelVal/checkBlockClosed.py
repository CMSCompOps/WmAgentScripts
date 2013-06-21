#!/usr/bin/env python

import sys,getopt,urllib,json

datasetpath = None
lfn = None
try:
    opts, args = getopt.getopt(sys.argv[1:], "", ["lfn=","dataset="])
except getopt.GetoptError:
    print 'Please specify dataset with --dataset or LFN with --lfn'
    sys.exit(2)

# check command line parameter
for opt, arg in opts :
    if opt == "--dataset" :
        datasetpath = arg
    if opt == "--lfn" :
        lfn = arg
    if opt == "--allSites" :
        allSites = 1
        
if datasetpath == None and lfn == None:
    print 'Please specify dataset with --dataset or LFN with --lfn'
    sys.exit(2)


if lfn == None :
    url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas?block=' + datasetpath + '%23*'
    result = json.load(urllib.urlopen(url))
    try:
        for block in result['phedex']['block']:
            name = block['name']
            is_open = block['is_open']
            if is_open == "y":
                print 'block:',name,'is open'
            else:
                print 'block:',name,'is closed'                
    except:
        print 'problems with dataset:',dataset
else:
    try:
        url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/filereplicas?lfn=' + lfn
        result = json.load(urllib.urlopen(url))
        for outerblock in result['phedex']['block']:
            blockname = outerblock['name'].split('#')[0] + "%23" + outerblock['name'].split('#')[1]
            url='https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas?block=' + blockname
            result = json.load(urllib.urlopen(url))
            for block in result['phedex']['block']:
                name = block['name']
                is_open = block['is_open']
                if is_open == "y":
                    print 'block:',name,'is open'
                else:
                    print 'block:',name,'is closed'                
    except:
        print 'problems with lfn:',lfn