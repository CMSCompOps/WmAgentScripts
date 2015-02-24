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

mysqlconn = MySQLdb.connect(host='dbod-altest1.cern.ch', user='relval', passwd="relval", port=5505)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd='relval')

curs = mysqlconn.cursor()

curs.execute("use "+dbname+";")

#workflow = line.rstrip('\n')
#curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

curs.execute("select * from batches")
batches=curs.fetchall()

colnames = [desc[0] for desc in curs.description]

for batch in batches:

    for name, value in zip(colnames, batch):
        print '    '+name.rstrip(' ')+': '+str(value)

    for name, value in zip(colnames, batch):
        if name == "useridday":
            useridday=value
        elif name == "useridmonth":    
            useridmonth=value
        elif name == "useridyear":    
            useridyear=value
        elif name == "useridnum":    
            useridnum=value
        elif name == "batch_id":
            batchid=value
        elif name == "description":
            description=value
        elif name == "announcement_title":
            title=value

    userid=useridyear+"_"+useridmonth+"_"+useridday+"_"+str(useridnum)        

    curs.execute("select workflow_name from workflows where batch_id = \""+ str(batch[0])+"\";")
    wfs=curs.fetchall()

    n_workflows=0
    n_completed=0
    max_completion_time=0
    for wf in wfs:
        n_workflows=n_workflows+1
        conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
        r1=conn.request('GET','/couchdb/wmstats/_all_docs?keys=["'+wf[0]+'"]&include_docs=true')
        r2=conn.getresponse()
        data = r2.read()
        if r2.status != 200:
            os.system('echo \"r2.status != 400\" | mail -s \"announcement_loop.py error 1\" andrew.m.levin@vanderbilt.edu --')
            print "problem connecting to wmstats, exiting"
            print r2.status
            sys.exit(0)
        s = json.loads(data)


        for status in s['rows'][0]['doc']['request_status']:
            if status['status'] == "completed":
                n_completed=n_completed+1
                if status['update_time'] > max_completion_time:
                    max_completion_time = status['update_time']
                break    

    print "(calendar.timegm(datetime.datetime.utcnow().utctimetuple()) - max_completion_time)/60.0/60.0 = " + str((calendar.timegm(datetime.datetime.utcnow().utctimetuple()) - max_completion_time)/60.0/60.0)
    
    print "datetime.datetime.now() = " + str(datetime.datetime.now())            
    print "n_workflows = " + str(n_workflows)
    print "n_completed = " + str(n_completed)

    if n_workflows != n_completed:
        continue

    #if batch[0] != 12:
    #    continue

    if (calendar.timegm(datetime.datetime.utcnow().utctimetuple()) - max_completion_time)/60.0/60.0 < 0.01:
        continue

    print batchid

    fname="brm/"+str(batchid)+".txt"
    print fname

    #remove the file if it already exists
    os.system("if [ -f "+fname+" ]; then rm "+fname+"; fi")

    print "putting workflows into a file"

    for wf in wfs:
        print wf[0]

        os.system("echo "+wf[0]+" >> "+fname)

    print "finished putting workflows into a file"     

    #if there is a '\r' character in the body of an e-mail, it does not get sent
    description=description.replace('\r','')

    os.system("bash announce_relvals.sh "+fname+" \""+description+"\" "+userid + ".txt | tee brm/announce_relvals_log.txt")    

    #remove the announcement e-mail file if it already exists
    os.system("if [ -f brm/announcement_email.txt ]; then rm brm/announcement_email.txt; fi")

    os.system("if [ -f brm/failure_information.txt ]; then rm brm/failure_information.txt; fi")

    os.system("python2.6 getFailureInformation.py "+fname+" >> brm/failure_information.txt")


    os.system("echo \"Dear all,\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \"A batch of relval workflows has finished.\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \"Batch ID:\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \""+userid+"\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \"List of datasets:\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \"http://cms-project-relval.web.cern.ch/cms-project-relval/relval_stats/"+userid+".txt\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \"Description:\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \""+description+"\" >> brm/announcement_email.txt")
    os.system("echo \"\" >> brm/announcement_email.txt")
    if int(os.popen("cat brm/failure_information.txt | wc -l").read().rstrip()) > 0:
        os.system("cat brm/failure_information.txt >> brm/announcement_email.txt")
        os.system("echo \"\" >> brm/announcement_email.txt")
    os.system("echo \"Best regards,\" >> brm/announcement_email.txt")
    os.system("echo \"Andrew\" >> brm/announcement_email.txt")

    os.popen("cat brm/announce_relvals_log.txt | mail -s \""+ title +"\" amlevin@mit.edu");

    if options.send_to_hn:
        os.popen("cat brm/announcement_email.txt | mail -s \""+ title +"\" hn-cms-relval@cern.ch -- -f amlevin@mit.edu");
    else:    
        os.popen("cat brm/announcement_email.txt | mail -s \""+ title +"\" andrew.m.levin@vanderbilt.edu");

    print "copying the workflows and the batch to the archive databases"    

    curs.execute("update batches set status=\"announced\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")

    curs.execute("select * from workflows where batch_id = \""+ str(batchid)+"\";")
    workflows_rows=curs.fetchall()

    curs.execute("select * from batches where batch_id = \""+ str(batchid)+"\";")
    batches_rows=curs.fetchall()

    #cannot just do batches_rows[0] since the current_status_start_time is not formatted correctly
    curs.execute("insert into batches_archive VALUES "+str(tuple(str(entry) for entry in batches_rows[0]))+";")

    for workflow_row in workflows_rows:
        curs.execute("insert into workflows_archive VALUES "+str(workflow_row)+";")

    print "deleting the workflows and the batch from the original databases"    
    curs.execute("delete from workflows where batch_id = \""+ str(batchid)+"\";")
    curs.execute("delete from batches where batch_id = \""+ str(batchid)+"\";")

    mysqlconn.commit()
    
#curs.execute("insert into batches set hn_req=\""+hnrequest+"\", announcement_title=\"myannouncementtitle\"")

