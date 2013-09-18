"""
__fixPhEDEx__

Created on Apr 3, 2013

@author: dballest
"""
import sys
import threading
import logging
import time
import os

try:
    from collections import defaultdict
    from WMCore.WMInit import connectToDB
    from WMCore.Database.DBFormatter import DBFormatter
    from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)

query = """
        SELECT lfn, blockname FROM dbsbuffer_file
        INNER JOIN dbsbuffer_block ON
            dbsbuffer_block.id = dbsbuffer_file.block_id
        WHERE in_phedex = 0 AND
              dbsbuffer_file.status = 'GLOBAL'
        """

modification = """
                UPDATE dbsbuffer_file
                SET in_phedex = 1
                WHERE lfn = :lfn
               """

def main():
    """
    _main_
    """
    # Start services
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    connectToDB()
    myPhEDEx = PhEDEx()
    myThread = threading.currentThread()
    print "Please remember to shutdown the PhEDExInjector first, you have 10 seconds before the script starts."
    time.sleep(10)

    # Get the files that the PhEDExInjector would look for
    formatter = DBFormatter(logging, myThread.dbi)
    formatter.sql = query
    results = formatter.execute()
    sortedBlocks = defaultdict(set)
    for lfn, block in results:
        sortedBlocks[block].add(lfn)

    # Check with block-level calls
    foundFiles = set()
    for block in sortedBlocks:
        result = myPhEDEx._getResult('data', args = {'block' : block}, verb = 'GET')
        for dbs in result['phedex']['dbs']:
            for dataset in dbs['dataset']:
                blockChunk = dataset['block']
                for blockInfo in blockChunk:
                    for fileInfo in blockInfo['file']:
                        if fileInfo['lfn'] in sortedBlocks[block]:
                            foundFiles.add(fileInfo['lfn'])
    if not foundFiles:
        print "I didn't find an abnormal file, feel free to panic!. Please contact a developer."
        return 0
    print "Found %d files that are already registered in PhEDEx but the buffer doesn't know" % len(foundFiles)
    print "Fixing them now..."
    # Fix it!
    binds = []
    for lfn in foundFiles:
        binds.append({'lfn' :lfn})
    formatter.dbi.processData(modification, binds,
                                        conn = None,
                                        transaction = False,
                                        returnCursor = False)
    print "Fixed them! :)"
    print "You can restart the PhEDExInjector now, have a nice day!"
    return 0

if __name__ == '__main__':
    sys.exit(main())
