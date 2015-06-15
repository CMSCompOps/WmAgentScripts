import MySQLdb
import sys
import os
import httplib
import json
import optparse
import datetime
import time
import calendar

parser = optparse.OptionParser()
parser.add_option('--correct_env',action="store_true",dest='correct_env')
parser.add_option('--sent_to_hn',action="store_true",dest='send_to_hn')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "

if not options.correct_env:
    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
    sys.exit(0)
    
url='cmsweb.cern.ch'


dbname = "relval"

while True:

    os.system("if [ -f relval_archive_monitor.txt ]; then rm relval_archive_monitor.txt; fi")
    
    sys.stdout = open('relval_archive_monitor.txt','a')

    print "last update: " + str(datetime.datetime.now())

    print ""

    conn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)

    #conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')

    curs = conn.cursor()
    
    curs.execute("use "+dbname+";")
    
    #workflow = line.rstrip('\n')
    #curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

    curs.execute("select * from batches_archive order by batch_id")
    batches=curs.fetchall()
    colnames = [desc[0] for desc in curs.description]

    for batch in batches:
        for name, value in zip(colnames, batch):
            if name == "batch_id":
                print name.rstrip(' ')+': '+str(value)
            else:
                print '    '+name.rstrip(' ')+': '+str(value)

        #print "    batch "+str(batch[0])
        #print "        hypernews request: "+str(batch[1])
        #print "        processing version: "+str(batch[6])
        #print "        status = " + str(batch[7])
        #print "        announcement e-mail title = " + str(batch[3])

    sys.stdout.flush()    
    os.system("cp relval_archive_monitor.txt /afs/cern.ch/user/r/relval//webpage/")

    sys.exit(0)
    time.sleep(60)

        
