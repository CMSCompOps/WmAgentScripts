"""
File       : DBSReader.py
Author     : Hasan Ozturk <haozturk AT cern dot com>
Description: General API for reading data from DBS
"""

import logging
#from dbs.apis.dbsClient import DbsApi

class DBS3Reader(object):
    """
    _DBSReader_
    General API for reading data from DBS
    """

    def __init__(self, url, logger=None, **contact):

        # instantiate dbs api object
        try:
            self.dbsURL = url.replace("cmsweb.cern.ch", "cmsweb-prod.cern.ch")
            #self.dbs = DbsApi(self.dbsURL, **contact)
            self.dbs = None
            self.logger = logger or logging.getLogger(self.__class__.__name__)
        except Exception as e:
            msg = "Error in DBSReader with DbsApi\n"
            msg += "%s\n" % format(e)
            raise Exception(msg)

def getDBSStatus(self, dataset):
    """
    The function to get the DBS status of outputs
    :param dataset: dataset name
    :return: DBS status of the given dataset
    """

    response = None
    try:
        response = self.dbs.listDatasets(dataset=dataset, dataset_access_type='*', detail=True)
    except Exception as ex:
        msg = "Exception while getting the status of following dataset on DBS: {} ".format(dataset)
        msg += "Error: {}".format(str(ex))
        self.logger.exception(msg)

    if response:
        dbsStatus = response[0]['dataset_access_type']
        self.logger.info("%s is %s", dataset, dbsStatus)
        return dbsStatus
    else:
        return None