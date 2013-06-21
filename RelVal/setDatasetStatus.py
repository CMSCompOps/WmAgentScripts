#!/usr/bin/env python
"""
_setDatasetStatus_

Given a dataset name and status, it sets the DBS status of this dataset to the status provided.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: setDatasetStatus.py,v 1.1 2009/07/14 13:56:03 direyes Exp $"

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsOptions import DbsOptionParser

import string,sys,os

def main():
    from optparse import OptionParser

    usage="usage: python setDatasetStatus.py --dataset=<DATASET_NAME> --status=<STATUS> --url=<DBSURL> {--files}"
    parser = OptionParser(usage=usage)

    parser.add_option('-d', '--dataset', dest='dataset', default=None, help='Dataset name')
    parser.add_option('-s', '--status', dest='status', default=None, help='This will be the new status of the dataset/files')
    parser.add_option('-D', '--url', dest='url', default='https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet', help='DBS URL')
    parser.add_option('-f', '--files', action="store_true", default=False, dest='files', help='Validate or invalidate all files in dataset')

    (opts, args) = parser.parse_args()

    if opts.dataset == None:
        print "--dataset option must be provided"
        print usage;
        sys.exit(1)
    if opts.status == None:
        print "--status option must be provided"
        print usage;
        sys.exit(1)
    if opts.url == None:
        print "--url option not provided."
        print "Using %s"%opts.url

    dbsargs = {'url' : opts.url}
    dbsapi = DbsApi(dbsargs)

    try:
        dbsapi.updateProcDSStatus(opts.dataset, opts.status)
    except Exception, ex:
        print "Caught Exception %s "  % str(ex)
        sys.exit(1)

    ### invalidating the files
    if opts.files:
        print "Files will be ",opts.status," as well."
        retrieveList=['retrive_status']
        files=dbsapi.listFiles(path=opts.dataset,retriveList=retrieveList)
        for f in files:
            dbsapi.updateFileStatus(f['LogicalFileName'], opts.status)

    print "Done"
    sys.exit(0)

if __name__ == "__main__":
    main()
