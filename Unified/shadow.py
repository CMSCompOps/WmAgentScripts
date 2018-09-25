from assignSession import *
import threading
import time
import assignor
import transferor
import completor
import checkor
import closor
import sys
import gc

from utils import reqmgr_url, display_time, checkMemory, Options

class UnifiedShadow(threading.Thread):
    def __init__(self, wfn):
        ## provide a wf name
        self.wfn = wfn
        ## create a lock that the shadow is indeed running
        self.start = time.gmtime()

    def run(self):
        countme = 2
        while True:
            countme -= 1
            if countme<0: break
            try:
                self.check()
            except Exception as e:
                print "Failed to check on",self.wfn
                print "Because of"
                print str(e)
            ## hold on before retrying
            usage = checkMemory()
            Nfreed = gc.collect()
            freed_usage = checkMemory()
            print Nfreed,"object collected and freed, from",usage,"to",freed_usage
            time.sleep( 5 )

    def check(self):
        n = time.gmtime()
        print "Checking on",self.wfn,time.asctime(n),", started since",display_time(time.mktime(n)-time.mktime(self.start))
        wfo = session.query(Workflow).filter(Workflow.name == self.wfn).first()
        if not wfo:
            print "This is embarrassing"
            return

        status = wfo.status
        print wfo.name,"in status",status
        if 'considered' in status:
            ## there are two things one can do.
            # first run assignor --early
            if not 'tried' in status:
                AO = Options(assignor.OParse(),early = True)
                _ = assignor.assignor(reqmgr_url, wfo.name , options = AO)
            # second run transferor
            TO = Options(transferor.OParse())
            _ = transferor.transferor(reqmgr_url, wfo.name, options = TO)
        elif status == 'staging':
            ## should not be running anything, as staging is not done wf by wf
            print "Workflow",wfo.name,"staging, be patient"
        elif status == 'staged':
            ## run assignor on it
            AO = Options(assignor.OParse())
            _ = assignor.assignor(reqmgr_url, wfo.name , options = AO)
        elif status == 'away':
            ## run completor on it
            _ = completor.completor(reqmgr_url, wfo.name)
            ## run checkor on it
            CO = checkor.options()
            _ = checkor.checkor(reqmgr_url, wfo.name, options = CO)
        elif 'assistance' in status:
            ## run checkor on it
            CO = checkor.options()
            _ = checkor.checkor(reqmgr_url, wfo.name, options = CO)
        elif status == 'close':
            ## run closor on it
            CO = closor.Options()
            _ = closor.closor(reqmgr_url, wfo.name, options = CO)
            
if __name__ == "__main__":
    shadow = UnifiedShadow( wfn = sys.argv[1] )
    shadow.run()
