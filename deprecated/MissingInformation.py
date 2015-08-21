#!/usr/bin/env python
import sys
import json
from deprecated import phedexSubscription
from das_client import get_data
#das_host='https://das.cern.ch'
das_host='https://cmsweb.cern.ch'
#das_host='https://cmsweb-testbed.cern.ch'

def getFileInformationBlock(das_url, block):
    files=[]
    query="file block="+block+" | grep file.block_name, file.name, file.checksum, file.adler32"
    das_data = get_data(das_host,query,0,0,0)
    if isinstance(das_data, basestring):
        result = json.loads(das_data)
    else:
        result = das_data
        
    if result['status'] == 'fail' :
        print 'DAS query failed with reason:',result['reason']
    else:
        preresult=result['data'] 
        for lfn in preresult:
            blockname=lfn['file'][0]['block_name']
            filename=lfn['file'][0]['name']
            adler32=lfn['file'][0]['adler32']
            chksum=lfn['file'][0]['checksum']
            newEntry=(blockname,filename,chksum,adler32)
            files.append(newEntry)
            return files
        
def main():
    args=sys.argv[1:]
    if not len(args)==1:
        print "usage file"
    filename=args[0]
    blocknames=deprecated.phedexSubscription.workflownamesfromFile(filename)
    files=[]
    for block in blocknames:
        files=files+getFileInformationBlock(das_host,block)
    for entry in files:
        print entry[0], entry[1], entry[2], entry[3]
    sys.exit(0);

if __name__ == "__main__":
    main()
