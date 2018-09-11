import glob
import os
import socket

base='/eos/cms/store/unified/www/public/globallocks.json.'
host = os.getenv('HOST',os.getenv('HOSTNAME',socket.gethostname()))
for flf in glob.glob('%s*'%base):
    lf = flf.replace(base,'')
    try:
        #print lf
        node,pid = lf.split('-')
        pid = pid.replace('.lock','')
        
        print "process",pid,"supposed to be running on",node
        print host
        if host == node:
            process = os.popen('ps -e -f | grep %s | grep -v grep'%pid).read()
            if not process:
                print "the lock",flf,"is a deadlock"
                os.system('rm -f %s'% flf)
        else:
            #print "run this on",node
            pass
    except:
        continue
