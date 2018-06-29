import os
from collections import defaultdict
import json

class dynamoClient(object):
    def __init(self):
        pass
    def _local_file(self, s):
        return '%s/%s_protected.db'%( '/data/unified-cache/', s)
        
    def get_site(self, s):
        print ("caching db for",s)
        #os.system('wget --connect-timeout 10 --read-timeout 180 -N http://dynamo.mit.edu/consistency/%s_protected.db -O %s '%(s , self._local_file(s)))
        d,f = self._local_file(s).rsplit('/',1)
        os.system('cd %s ; wget -q -N http://dynamo.mit.edu/consistency/%s_protected.db ; cd -'%(d, s ))
        os.system("touch -t `date +'%%Y%%m%%d%%H%%M.%%S'` %s"% self._local_file(s))

    def files_in_dir(self, dirs):
        files = defaultdict( list)
        for site,dirs in dirs.items():
            self.get_site(site)
            for dir in dirs:
                print "checking db",self._local_file(site),"for",dir
                com = 'echo "SELECT file FROM files JOIN directories ON id=dir WHERE dirname=\'%s\';" | sqlite3 %s'%( dir, self._local_file(site) )
                print "com:",com
                for l in os.popen(com).read().split('\n'):
                    if 'root' in l:
                        files[site].append( '%s/%s'% ( dir, l ))
        return files


if __name__ == "__main__":
    DC = dynamoClient()
    res = DC.files_in_dir( {"T2_US_Wisconsin": ["/store/unmerged/PhaseIISummer17wmLHEGENOnly/TTTT_TuneCUETP8M2_14TeV-madgraph-pythia8/GEN/93X_upgrade2023_realistic_v5_ext1-v3/60001"]})
    print json.dumps( res, indent=2)

    
            
    
