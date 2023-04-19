#!/usr/bin/env python
"""
Encapsulates requests to Rucio API
Requieres:
    rucio-client
Environment:
    export X509_USER_PROXY=/tmp/x509up_$UID
    export RUCIO_HOME=~/.local/
    ${RUCIO_HOME}/rucio.cfg
"""

from rucio.client import Client
from WMCore.Services.CRIC.CRIC import CRIC

class RucioClient(Client):
    """
    A wrapper class for the Rucio client.
    """
    def __init__(self, **kwargs):
        """
        Default configuration provided directly into the constructor to avoid
        the need of an external configuration file.
        All arguments passed to the constructor supersede the defaults.
        """

        defaultConfig = {
            'rucio_host': 'http://cms-rucio.cern.ch',
            'auth_host': 'https://cms-rucio-auth.cern.ch',
            'auth_type': 'x509_proxy',
            'ca_cert': '/etc/grid-security/certificates/',
            'account': 'haozturk'
        }

        defaultConfig.update(kwargs)

        super(RucioClient, self).__init__(**defaultConfig)
        self.scope = 'cms'

    def getFileCountDataset(self, dataset):
        """
        Returns the number of files registered in Rucio
        """
        try:
            files = list(self.list_files(self.scope, dataset))
        except Exception as e:
            print((str(e)))
            return 0
        return len(files)

    def getFileNamesDataset(self, dataset):
        """
        Returns a set of file names in a dataset registered in Rucio
        """
        try:
            files = list(self.list_files(self.scope, dataset))
        except Exception as e:
            print((str(e)))
            return []
        fileNames = [_file['name'] for _file in files]
        return fileNames

    def getBlockNamesDataset(self, dataset):
        """
        Returns a set of block names in a dataset registerd in Rucio
        """
        try:
            blockNames = [block['name'] for block in self.list_content(self.scope, dataset)]
        except Exception as e:
            print((str(e)))
            return []
        return blockNames

    def getFileCountBlock(self, block):
        """
        Returns the number of files in a block registered in Rucio
        """
        try:
            numFiles = self.get_metadata(self.scope, block)['length']
            if numFiles is None:
                raise Exception("block length in rucio is None")
        except Exception as e:
            print((str(e)))
            return 0
        return numFiles

    def getFileCountPerBlock(self, dataset):
        """
        Returns the number of files per block in a dataset registered in Rucio
        """
        # we need blocks to be a list of tuples so we can create a set out of this
        try:
            blocks = []
            for block in self.getBlockNamesDataset(dataset):
                blocks.append((block, self.getFileCountBlock(block)))
        except Exception as e:
            print((str(e)))
            return []
        return blocks

    def getDatasetLocationsByAccount(self, dataset, account):
        """
        Returns the dataset locations for the given account in terms of computing element (not RSE name). 
        This function assumes that the returned RSE expression includes only one RSE 
        """
        try:
            rules = self.list_did_rules(self.scope, dataset)
            for rule in rules:
                if rule['account'] == account:
                    RSEs = rule['rse_expression'].split("|")

            #RSE to CE conversion
            cric = CRIC()
            CEs = cric.PNNstoPSNs(RSEs)
        except Exception as e:
            print("Exception while getting the dataset location")
            print((str(e)))
            return []
        return CEs

    def getDatasetLocationsByAccountAsRSEs(self, dataset, account):
        """
        Returns the dataset locations for the given account in terms of computing element (not RSE name).
        This function assumes that the returned RSE expression includes only one RSE
        """
        try:
            rules = self.list_did_rules(self.scope, dataset)
            RSEs = set()
            for rule in rules:
                if rule['account'] == account:
                    RSEs = RSEs.union(set(rule['rse_expression'].split("|")))

            return list(RSEs)
        except Exception as e:
            print("Exception while getting the dataset location")
            print((str(e)))
            return []

