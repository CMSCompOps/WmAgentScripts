#!/usr/bin/env python
"""
_setDatasetStatus_

Give the dataset path, the new status and DBS Instance url (writer), it will
set the new status.

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: setDatasetStatus.py,v 1.1 2009/07/14 13:56:03 direyes Exp $"

import sys
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)

try:
    if len(sys.argv) < 3 or opts.__dict__.get('url', None) is None:
        print "Missing dataset path"
        print "USAGE: "+ sys.argv[0] + " /specify/dataset/path" + \
            " newStatus" + " --url=<DBS_Instance_URL>"
        sys.exit(1)
    path = sys.argv[1]; newStatus = sys.argv[2]
    api.updateProcDSStatus(path, newStatus)

except DbsApiException, ex:
    print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
    if ex.getErrorCode() not in (None, ""):
        print "DBS Exception Error Code: ", ex.getErrorCode()

print "Done"

