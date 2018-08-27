#!/usr/bin/env python                                                                                                                      
import os
import json
import time
from utils import sendEmail, sendLog, monitor_eos_dir, eosRead
import sys

## get the wf from many places

c=0   
while True:
    c+=1
    if len(sys.argv)>1 and c>int(sys.argv[1]): break

    ow = open('/afs/cern.ch/user/v/vlimant/public/ops/retrievers.json')
    whos = json.loads(ow.read())
    ow.close()
    print json.dumps( whos , indent=2)
    print "Can get stuff in"
    wfs = []
    indexed_this_round = set()
    for who in whos:
        try:
            rw = open('/afs/cern.ch/user/%s/%s/public/ops/retrieve.json'%(who[0],who))
            wfs.extend(json.loads(rw.read()))
            rw.close()
        except:
            print who,"is not ready"
        
    for (wf,lf) in wfs:
        print "#"*30
        print "Getting",wf.strip(),lf.strip()
        expose_all='%s/logmapping/%s/'%(monitor_eos_dir, wf)
        os.system('mkdir -p %s'% expose_all)

        agent,jobid = None, None
        if ":" in lf:
            agent,jobid = lf.split(":")
            if not 'vocms' in agent: 
                print "cannot do non cern agents"
                continue
            ## get the archive from the agent
            com = 'ssh %s %s/WmAgentScripts/Unified/exec_expose.sh %s %s %s %s %s %s'%(
                agent,
                '/afs/cern.ch/user/c/cmst2/Unified/',
                wf,
                jobid,
                0,
                '/afs/cern.ch/user/c/cmst2/Unified/',
                monitor_eos_dir+'/logmapping/',
                'ATask'
                )
            ## parse the condor logs for the tar.gz
            condor_dir ='%s/logmapping/condorlogs/%s/0/ATask/%s/%s_%s/Job_%s'%(monitor_eos_dir,
                                                                                                                wf,
                                                                                                                jobid[:3],
                                                                                                                agent.split('.')[0],
                                                                                                                jobid,
                                                                                                                jobid)
            if not os.path.isdir( condor_dir ):
                print "Ex: ",com
                os.system(com)
                
            #outs = os.popen('find %s -name "*.out"'% ( condor_dir )).read()
            #print outs
            look_in_files = []
            look_in_files.extend( filter(None,os.popen('find %s -name "*.log"'% ( condor_dir )).read().split('\n')) )
            look_in_files.extend( filter(None,os.popen('find %s -name "*.out"'% ( condor_dir )).read().split('\n')) )
            lfs = []
            flfs = []
            for out in look_in_files:
                print "Looking in file",out
                fhr = eosRead( out , trials=2 )
                if not fhr: continue
                for line in fhr.split('\n'):
                    if 'logArchive.tar.gz' in line:
                        fullpath = filter(lambda w : 'logArchive.tar.gz' in w, line.split())[0]
                        alf = fullpath.split('/')[-1].strip()
                        if not '/' in fullpath or alf == 'logArchive.tar.gz' or ':' in fullpath:
                            print fullpath,"not satisfactory to find log file name"
                            #sendLog('efficiencor','check on the logs of efficiencor, for %s'%(wf),level='critical')
                            alf = None
                            fullpath = None
                            continue
                        print "found log name", alf,fullpath,"in condor log",out.split('/')[-1]
                        #print "full name",fullpath
                        lfs.append( alf )
                        flfs.append( fullpath )
                        break

            if not lfs:
                print "Could not find trace of a log file for",lf
                continue
            print "found",lfs
            #lf = sorted(lfs)[-1]
            lf = lfs[-1]
            fullpath = flfs[-1]
            print "taking",lf
        else:
            ### already a log filename
            continue

        ## then do the rest
        not_found = (lf and 'nothing found' in os.popen('Unified/whatLog.py --workflow %s --log %s'%(wf,lf)).read())
        nothing_indexed = ('nothing found' in os.popen('Unified/whatLog.py --workflow  %s'%wf).read())
        not_already_indexed = (wf not in indexed_this_round)

        print ("log index NOT found" if not_found else "log index found"),"for",lf
        print ("log NOT indexed" if nothing_indexed else "log indexed"),"for",wf

        if (not_found or nothing_indexed) and not_already_indexed:
            print "Making the full query from eos to create the index of ",wf
            indexed_this_round.add( wf )
            com = 'Unified/createLogDB.py --workflow %s'% (wf)
            print "Ex :",com
            os.system(com) ## heavy

        ## list things     ## put the text somewhere useful
        mapf = '%s/mapping.txt'% expose_all
        if not os.path.isfile( mapf ) or not_found or nothing_indexed:
            com = 'Unified/whatLog.py --workflow %s --where > %s'%( wf,
                                                                    mapf)
            print "Ex :",com
            os.system(com)
        else:
            print "mapping file already there"


        if lf:
            final_dest = '%s/%s'%( expose_all, lf)
            print "\t and specifically",lf
            if os.path.isfile( final_dest ):
                print final_dest,"already on the web"
                continue
            ## show that one
            com = 'Unified/whatLog.py --workflow  %s --log %s > %s/%s.txt'%(wf, lf, expose_all, lf)
            print 'Ex:',com
            os.system(com)
            ## get it locally
            com = 'Unified/whatLog.py --workflow  %s --log %s --get '%( wf, lf )
            print 'Ex:',com
            os.system(com)
            lfile = os.popen('find /tmp/vlimant/ -name "*%s"'% lf ).read().replace('\n','')
            print lfile,"is mean to be the local file"
            if not(lfile and os.path.isfile( lfile )):
                print "file not found, revert to xrd"
                ## do you want to get it from xrdcp ?
                com = 'XRD_REQUESTTIMEOUT=10 xrdcp root://cms-xrd-global.cern.ch/%s /tmp/vlimant/%s'%( fullpath , lf)
                print 'Ex:',com
                os.system(com)
            lfile = os.popen('find /tmp/vlimant/ -name "*%s"'% lf ).read().replace('\n','')
            print lfile,"is mean to be the local file"

            if lfile and os.path.isfile( lfile ):
                com = 'cp %s %s/.'%( lfile, expose_all)
                print 'Ex:',com
                os.system( com )
                if agent and jobid:
                    com = 'cp %s %s/%s_%s.tar.gz'%( lfile, expose_all, agent,jobid)
                    print 'Ex:',com
                    os.system( com )

                ##sendEmail("%s is ready"%lf,"Get the file at http://cms-unified.web.cern.ch/cms-unified/logmapping/%s"%(wf), destination= [whos[who]])
                print "Got the file",lf
            else:
                no_file = True
                print "no file yet retreived"
                ##sendEmail("%s is ready"%lf,"Get the file at http://cms-unified.web.cern.ch/cms-unified/logmapping/%s"%(wf), destination= [whos[who]])
           
    print "waiting a bit"
    time.sleep(30)
