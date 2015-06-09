import reqMgrClient

import smtplib
import email

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate

import MySQLdb 
import sys
import os
import httplib
import json
import optparse
import datetime
import time
import calendar
import jobFailureInformation

import utils

import closeOutTaskChain
import getRelValDsetNames
import makeStatisticsTable

import setDatasetStatusDBS3

parser = optparse.OptionParser()
parser.add_option('--sent_to_hn',action="store_true",dest='send_to_hn')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "

url='cmsweb.cern.ch'

dbname = "relval"

#workflow = line.rstrip('\n')
#curs.execute("insert into workflows set hn_req=\""+hnrequest+"\", workflow_name=\""+workflow+"\";")

while True:

    mysqlconn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)

    curs = mysqlconn.cursor()

    curs.execute("use "+dbname+";")

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
                print url+'/couchdb/wmstats/_all_docs?keys=["'+wf[0]+'"]&include_docs=true'
                print "problem connecting to wmstats, exiting"
                print r2.status
                sys.exit(0)
            s = json.loads(data)

            for status in s['rows'][0]['doc']['request_status']:
                if status['status'] == "completed" or status['status'] == "force-complete":
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

        #string="2015_06_04_1"

        #if not (string.split('_')[0] == useridyear and string.split('_')[1] == useridmonth and string.split('_')[2] == useridday and string.split('_')[3] == str(useridnum)):
        #    continue

        #if batch[0] != 222:
        #    continue

        print batchid

        fname="brm/"+str(batchid)+".txt"
        print fname

        #remove the file if it already exists
        os.system("if [ -f "+fname+" ]; then rm "+fname+"; fi")

        print "putting workflows into a file"

        wf_list = []
        
        for wf in wfs:
            print wf[0]

            wf_list.append(wf[0])

            #os.system("echo "+wf[0]+" >> "+fname)

        print "finished putting workflows into a file"     

        #if there is a '\r' character in the body of an e-mail, it does not get sent
        description=description.replace('\r','')

        dsets_stats_tmp=os.popen("mktemp").read().rstrip('\n')
        dsets_tmp_cern=os.popen("mktemp").read().rstrip('\n')
        dsets_tmp_fnal=os.popen("mktemp").read().rstrip('\n')
        dsets_tmp_fnal_disk=os.popen("mktemp").read().rstrip('\n')
        dsets_tmp_cern_alcareco=os.popen("mktemp").read().rstrip('\n')

        closeOutTaskChain.close_out_wf_list(wf_list)

        dset_nevents_list=getRelValDsetNames.getDsetNamesAndNevents(wf_list)

        makeStatisticsTable.makeStatisticsTable(dset_nevents_list, userid+".txt")

        ret=os.system("cp "+userid+".txt /afs/cern.ch/user/r/relval/webpage/relval_stats/"+userid+".txt")

        if ret != 0:
            os.system('echo \"'+userid+'\" | mail -s \"announcement_loop.py error 2\" andrew.m.levin@vanderbilt.edu')


        dsets_list = []    
        dsets_fnal_disk_list = []
        dsets_cern_disk_list = []

        for dset_nevents in dset_nevents_list:
            dsets_list.append(dset_nevents[0])
            

        for dset in dsets_list:

            #print dset.split('/')

            if dset.split('/')[3] != "RECO" and dset.split('/')[3] != "ALCARECO":
                dsets_cern_disk_list.append(dset)

            if dset.split('/')[3] == "GEN-SIM":
                dsets_fnal_disk_list.append(dset)

            if dset.split('/')[3] == "GEN-SIM-DIGI-RAW":
                dsets_fnal_disk_list.append(dset)

            if dset.split('/')[3]  == "GEN-SIM-RECO":
                dsets_fnal_disk_list.append(dset)

            if "RelValTTBar" in dset.split('/')[1] and "TkAlMinBias" in dset.split('/')[2] and dset.split('/')[3] != "ALCARECO":
                dsets_cern_disk_list.append(dset)

            if "MinimumBias" in dset.split('/')[1] and "SiStripCalMinBias" in dset.split('/')[2] and dset.split('/')[3] != "ALCARECO":
                dsets_cern_disk_list.append(dset)

        result=utils.makeReplicaRequest("cmsweb.cern.ch", "T2_CH_CERN", dsets_cern_disk_list, "relval datasets",group="RelVal")
        if result != None:
            phedexid = result['phedex']['request_created'][0]['id']
            utils.approveSubscription("cmsweb.cern.ch",phedexid)

        result=utils.makeReplicaRequest("cmsweb.cern.ch", "T0_CH_CERN_MSS", dsets_list, "relval datasets", group = "RelVal")
        if result != None:
            phedexid = result['phedex']['request_created'][0]['id']
            utils.approveSubscription("cmsweb.cern.ch",phedexid)

        result=utils.makeReplicaRequest("cmsweb.cern.ch", "T1_US_FNAL_Disk", dsets_fnal_disk_list, "relval datasets", group = "RelVal")
        #phedexid = result['phedex']['request_created'][0]['id']
        #utils.approveSubscription("cmsweb.cern.ch",phedexid)

        for dset in dsets_list:
            setDatasetStatusDBS3.setStatusDBS3("https://cmsweb.cern.ch/dbs/prod/global/DBSWriter", dset, "VALID", True)

        for wf in wf_list:
            reqMgrClient.closeOutWorkflow("cmsweb.cern.ch",wf)
            reqMgrClient.announceWorkflow("cmsweb.cern.ch",wf)


        #remove the announcement e-mail file if it already exists
        os.system("if [ -f brm/announcement_email.txt ]; then rm brm/announcement_email.txt; fi")

        os.system("if [ -f brm/failure_information.txt ]; then rm brm/failure_information.txt; fi")

        [istherefailureinformation,return_string]=jobFailureInformation.getFailureInformation(wf_list,"brm/failure_information.txt")

        msg = MIMEMultipart()
        reply_to = []
        send_to = ["andrew.m.levin@vanderbilt.edu"]
        #send_to = ["hn-cms-dataopsrequests@cern.ch","andrew.m.levin@vanderbilt.edu"]
        #send_to = ["hn-cms-hnTest@cern.ch"]
            
        #msg['In-Reply-To'] = hn_message_id
        #msg['References'] = hn_message_id
            
        msg['From'] = "amlevin@mit.edu"
        msg['reply-to'] = COMMASPACE.join(reply_to)
        msg['To'] = COMMASPACE.join(send_to)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = title
        msg['Message-ID'] = email.Utils.make_msgid()

        messageText="Dear all,\n"
        messageText=messageText+"\n"
        messageText=messageText+"A batch of relval workflows has finished.\n"
        messageText=messageText+"\n"
        messageText=messageText+"Batch ID:\n"
        messageText=messageText+"\n"
        messageText=messageText+userid+"\n"
        messageText=messageText+"\n"
        messageText=messageText+"List of datasets:\n"
        messageText=messageText+"\n"
        messageText=messageText+"http://cms-project-relval.web.cern.ch/cms-project-relval/relval_stats/"+userid+".txt\n"
        messageText=messageText+"\n"
        messageText=messageText+"Description:\n"
        messageText=messageText+"\n"
        messageText=messageText+description.rstrip('\n')
        messageText=messageText+"\n"
        #messageText=messageText+"\n"
        if istherefailureinformation:
            messageText=messageText+"\n"
            messageText=messageText+return_string
            messageText=messageText+"\n"
        messageText=messageText+"\n"
        messageText=messageText+"RelVal Batch Manager"

        try:
            msg.attach(MIMEText(messageText))
            smtpObj = smtplib.SMTP()
            smtpObj.connect()
            smtpObj.sendmail("amlevin@mit.edu", send_to, msg.as_string())
            smtpObj.close()
        except Exception as e:
            print "Error: unable to send email: %s" %(str(e))

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
        curs.execute("delete from datasets where batch_id = \""+str(batchid)+"\";")


        mysqlconn.commit()

    time.sleep(100)
    
