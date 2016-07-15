#!/usr/bin/env python
"""
_setDatasetStatus_

Given a dataset name and status, it sets the DBS status of this dataset to the status provided.

"""

import string,sys,os

def setStatusDBS2(url2, dataset, newStatus, files):
    from DBSAPI.dbsApi import DbsApi
    from DBSAPI.dbsOptions import DbsOptionParser

    dbsargs = {'url' : url2}
    dbsapi = DbsApi(dbsargs)

    try:
        dbsapi.updateProcDSStatus(dataset, newStatus)
    except Exception, ex:
        print "Caught DBS2 Exception %s "  % str(ex)
        sys.exit(1)

    ### invalidating files as well
    if files:
        if newStatus in ['DELETED', 'DEPRECATED', 'INVALID']:
            newStatus = 'INVALID'
        elif newStatus in ['PRODUCTION', 'VALID']:
            newStatus = 'VALID'
        else:
            print "Sorry, I don't know this status and you cannot set files to %s" % newStatus
            print "Only the dataset was changed. Quitting the program!"
            sys.exit(1)
        print "Files will be set to:",newStatus,"in DBS2"
        retrieveList=['retrive_status']
        files=dbsapi.listFiles(path=dataset,retriveList=retrieveList)
        for f in files:
            dbsapi.updateFileStatus(f['LogicalFileName'], newStatus)


def setStatusDBS3(url3, dataset3, newStatus, files):
    from dbs.apis.dbsClient import DbsApi

    dbsapi = DbsApi(url=url3)

    try:
        dbsapi.updateDatasetType(dataset=dataset3, dataset_access_type=newStatus)
    except Exception, ex:
        print "Caught Exception %s " % str(ex)
        sys.exit(1)

    ### invalidating the files
    if files:
        if newStatus in ['DELETED', 'DEPRECATED', 'INVALID']:
            file_status = 0
        elif newStatus in ['PRODUCTION', 'VALID']:
            file_status = 1
        else:
            print "Sorry, I don't know this state and you cannot set files to %s" % newStatus
            print "Only the dataset was changed. Quitting the program!"
            sys.exit(1)
        print "Files will be set to:",file_status,"in DBS3"
        files = dbsapi.listFiles(dataset=dataset3)
        for this_file in files:
            dbsapi.updateFileStatus(logical_file_name=this_file['logical_file_name'],is_file_valid=file_status)


def main():
    from optparse import OptionParser

    usage="usage: python setDatasetStatus.py --dataset=<DATASET_NAME> --status=<STATUS> {--files}"
    parser = OptionParser(usage=usage)
    #parser.add_option('--correct_env',action="store_true",dest='correct_env')
    parser.add_option('-d', '--datasets', dest='dsets', default=None, help='file with the list of dataset names, or comma separated list of dataset')
    parser.add_option('-s', '--status', dest='status', default=None, help='This will be the new status of the dataset/files')
    parser.add_option('-f', '--files', action="store_true", default=False, dest='files', help='Validate or invalidate all files in dataset')

    (opts, args) = parser.parse_args()

    #command=""
    #for arg in sys.argv:
    #    command=command+arg+" "

    #if not opts.correct_env:
    #    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; source /tmp/relval/sw/comp.pre/slc6_amd64_gcc481/cms/dbs3-client/3.2.10/etc/profile.d/init.sh; python2.6 "+command + "--correct_env")
    #    sys.exit(0)
        
    if opts.dsets == None:
        print "--datasets option must be provided"
        print usage;
        sys.exit(1)
    if opts.status == None:
        print "--status option must be provided"
        print usage;
        sys.exit(1)

    #setStatusDBS2('https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet', opts.dataset, opts.status, opts.files)
    #setStatusDBS3('https://dbs3-testbed.cern.ch/dbs/prod/global/DBSWriter', opts.dataset, opts.status, opts.files)

    try:
        f = open(opts.dsets, 'r')    
    except:
        f = opts.dsets.split(',')

    for line in f:
        dset=line.strip('\n')
        setStatusDBS3('https://cmsweb.cern.ch/dbs/prod/global/DBSWriter', dset, opts.status, opts.files)

    print "Done"
    sys.exit(0)

if __name__ == "__main__":
    main()
