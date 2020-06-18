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

import os
if not os.getenv('RUCIO_HOME'): os.environ['RUCIO_HOME']='/data/unified/WmAgentScripts/Unified/'
from rucio.client import Client


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
            'account': 'unified' if os.getenv('USER')=='cmsunified' else os.getenv('USER')
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
            print(str(e))
            return 0
        return len(files)

    def getFileNamesDataset(self, dataset):
        """
        Returns a set of file names in a dataset registered in Rucio
        """
        try:
            files = list(self.list_files(self.scope, dataset))
        except Exception as e:
            print(str(e))
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
            print(str(e))
            return []
        return blockNames

    def getFileCountBlock(self, block):
        """
        Returns the number of files in a block registered in Rucio
        """
        try:
            numFiles = self.get_metadata(self.scope, block)['length']
        except Exception as e:
            print(str(e))
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
            print(str(e))
            return 0
        return blocks

