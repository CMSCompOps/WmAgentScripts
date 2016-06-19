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
import collect_job_failure_information
import assistance_decision
import print_job_failure_information

import utils

import too_many_events_check
import collect_dsets_and_nevents
import print_dsets_and_nevents

import setDatasetStatusDBS3

url='cmsweb.cern.ch'

dbname = "relval"

def main():

    mysqlconn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)

    curs = mysqlconn.cursor()

    curs.execute("use "+dbname+";")

    #curs.execute("lock tables batches write, batches_archive write, workflows write, workflows_archive write, datasets write, clone_reinsert_requests write")

    curs.execute("select * from batches")
    batches=curs.fetchall()
    
    batches_colnames = [desc[0] for desc in curs.description]

    for batch in batches:

        batch_dict= dict(zip(batches_colnames, batch))

        if batch_dict["status"] != "assigned":
            continue

        userid=batch_dict["useridyear"]+"_"+batch_dict["useridmonth"]+"_"+batch_dict["useridday"]+"_"+str(batch_dict["useridnum"])+"_"+str(batch_dict["batch_version_num"])

        print "   userid ==> "+userid

        curs.execute("select workflow_name from workflows where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+ "\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num ="+str(batch_dict["batch_version_num"])+";")
        wfs=curs.fetchall()

        n_workflows=0
        n_completed=0
        for wf in wfs:
            n_workflows=n_workflows+1
            conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
            r1=conn.request('GET','/reqmgr2/data/request?name='+wf[0],headers={"Accept": "application/json"})
            r2=conn.getresponse()
            data = r2.read()
            if r2.status != 200:
                time.sleep(10)
                #try it again
                conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request('GET','/reqmgr2/data/request?name='+wf[0],headers={"Accept": "application/json"})
                r2=conn.getresponse()
                data = r2.read()
                if r2.status != 200:
                    time.sleep(10)
                    #try it a third time
                    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                    r1=conn.request('GET','/reqmgr2/data/request?name='+wf[0],headers={"Accept": "application/json"})
                    r2=conn.getresponse()
                    data = r2.read()
                    if r2.status != 200:
                        os.system('echo \"'+wf[0]+'\" | mail -s \"announcor.py error 1\" andrew.m.levin@vanderbilt.edu')
                        sys.exit(1)
            s = json.loads(data)

            for status in s['result'][0][wf[0]]['RequestTransition']:
                if status['Status'] == "completed" or status['Status'] == "force-complete":
                    #conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                    #r1=conn.request('GET',"/wmstatsserver/data/isfinished/"+wf[0],headers={"Accept": "application/json"})
                    #r2=conn.getresponse()
                    #data = r2.read()        
                    #s = json.loads(data)
                    
                    #if s['result'][0] == "true":
                    #    n_completed=n_completed+1
                    n_completed=n_completed+1
                    break    

        print "datetime.datetime.now() = " + str(datetime.datetime.now())            
        print "n_workflows = " + str(n_workflows)
        print "n_completed = " + str(n_completed)

        if n_workflows != n_completed:
            continue

        #string="2016_04_11_1_0"

        #if not (string.split('_')[0] == batch_dict["useridyear"] and string.split('_')[1] == batch_dict["useridmonth"] and string.split('_')[2] == batch_dict["useridday"] and string.split('_')[3] == str(batch_dict["useridnum"]) and string.split('_')[4] == str(batch_dict["batch_version_num"])):
        #    continue

        wf_list = []
        
        for wf in wfs:
            print wf[0]

            wf_list.append(wf[0])

        job_failure_information=collect_job_failure_information.collect_job_failure_information(wf_list)

        needs_assistance = assistance_decision.assistance_decision(job_failure_information)

        if needs_assistance:
            curs.execute("update batches set status=\"assistance\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+ "\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num ="+str(batch_dict["batch_version_num"])+";")
            mysqlconn.commit()
            os.system('echo \"batch_id: '+userid+'\" | mail -s \"a batch of relval workflows needs assistance\" andrew.m.levin@vanderbilt.edu')
            continue

        #if there is a '\r' character in the body of an e-mail, it does not get sent
        description=batch_dict["description"].replace('\r','')

        for wf in wf_list:
            too_many_events_check.too_many_events_check(wf)

        dset_nevents_list=collect_dsets_and_nevents.collect_dsets_and_nevents(wf_list)

        print_dsets_and_nevents.print_dsets_and_nevents(dset_nevents_list, userid+".txt")

        ret=os.system("cp "+userid+".txt /afs/cern.ch/user/r/relval/webpage/relval_stats/"+userid+".txt")

        if ret == 0:
            os.system("rm "+userid+".txt")
        else:
            os.system('echo \"'+userid+'\" | mail -s \"announcor.py error 2\" andrew.m.levin@vanderbilt.edu')
            sys.exit(0)

        dsets_list = []    

        for dset_nevents in dset_nevents_list:
            dsets_list.append(dset_nevents[0])

        for dset in dsets_list:
            setDatasetStatusDBS3.setStatusDBS3("https://cmsweb.cern.ch/dbs/prod/global/DBSWriter", dset, "VALID", True)

        for wf in wf_list:
            reqMgrClient.closeOutWorkflow("cmsweb.cern.ch",wf)
            reqMgrClient.announceWorkflow("cmsweb.cern.ch",wf)

        msg = MIMEMultipart()
        reply_to = []
        #send_to = ["andrew.m.levin@vanderbilt.edu","andrew.m.levin.filter1@gmail.com"]
        send_to = ["hn-cms-relval@cern.ch","andrew.m.levin@vanderbilt.edu","andrew.m.levin.filter1@gmail.com"]
        #send_to = ["hn-cms-hnTest@cern.ch"]
            
        #msg['In-Reply-To'] = hn_message_id
        #msg['References'] = hn_message_id
            
        msg['From'] = "amlevin@mit.edu"
        msg['reply-to'] = COMMASPACE.join(reply_to)
        msg['To'] = COMMASPACE.join(send_to)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = batch_dict["announcement_title"]
        msg['Message-ID'] = email.Utils.make_msgid()

        messageText="Dear all,\n"
        messageText=messageText+"\n"
        messageText=messageText+"A batch of relval workflows has finished.\n"
        messageText=messageText+"\n"
        messageText=messageText+"Batch ID:\n"
        messageText=messageText+"\n"
        messageText=messageText+userid+"\n"
        if batch_dict["batch_version_num"] > 0:
            messageText=messageText+"\n"
            messageText=messageText+"original workflow name ==> clone name:\n"
            messageText=messageText+"\n"
            curs.execute("select workflow_name,original_workflow_name from workflows where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+ "\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num ="+str(batch_dict["batch_version_num"])+";")
            workflows=curs.fetchall()
            for workflow in workflows:
                messageText=messageText+workflow[1] + " ==> "+workflow[0] + "\n"
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
        [istherefailureinformation,return_string]=print_job_failure_information.print_job_failure_information(job_failure_information)

        if istherefailureinformation:
            messageText=messageText+"\n"
            messageText=messageText+return_string
            messageText=messageText+"\n"
        messageText=messageText+"\n"
        messageText=messageText+"RelVal Batch Manager"

        #put the announcement message into an e-mail to the relval hypernews and also in a url
        output_file = open("/afs/cern.ch/user/r/relval/webpage/relval_announcements/"+userid+".txt", 'w')

        output_file.write(messageText)

        try:
            msg.attach(MIMEText(messageText))
            smtpObj = smtplib.SMTP()
            smtpObj.connect()
            smtpObj.sendmail("amlevin@mit.edu", send_to, msg.as_string())
            smtpObj.close()
        except Exception as e:
            print "Error: unable to send email: %s" %(str(e))

        dsets_fnal_disk_list = []
        dsets_cern_disk_list = []
            
        for dset in dsets_list:

            #print dset.split('/')

            # we were asked to transfer some specific datasets to the cern tier 2
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

        result=utils.makeReplicaRequest("cmsweb.cern.ch", "T1_US_FNAL_Disk", dsets_fnal_disk_list, "relval datasets", group = "RelVal")
        if result != None:
            phedexid = result['phedex']['request_created'][0]['id']
            utils.approveSubscription("cmsweb.cern.ch",phedexid)

        result=utils.makeMoveRequest("cmsweb.cern.ch", "T0_CH_CERN_MSS", dsets_list, "relval datasets", group = "RelVal")
        if result != None:
            phedexid = result['phedex']['request_created'][0]['id']
            #even if you disapprove the subscription at the source, it will still deleted the datasets that are at the source but not subscribed their
            utils.disapproveSubscription("cmsweb.cern.ch",phedexid,["T2_CH_CERN"])
            utils.disapproveSubscription("cmsweb.cern.ch",phedexid,["T1_US_FNAL_Disk"])
            utils.disapproveSubscription("cmsweb.cern.ch",phedexid,["T1_FR_CCIN2P3_Disk"])
            utils.disapproveSubscription("cmsweb.cern.ch",phedexid,["T1_DE_KIT_Disk"])
            utils.approveSubscription("cmsweb.cern.ch",phedexid,["T0_CH_CERN_MSS"])

        #phedexid = result['phedex']['request_created'][0]['id']
        #utils.approveSubscription("cmsweb.cern.ch",phedexid)

        curs.execute("update batches set status=\"announced\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where useridyear = \""+batch_dict["useridyear"]+"\" and useridmonth = \""+batch_dict["useridmonth"]+ "\" and useridday = \""+batch_dict["useridday"]+"\" and useridnum = "+str(batch_dict["useridnum"])+" and batch_version_num ="+str(batch_dict["batch_version_num"])+";")
        mysqlconn.commit()

    #curs.execute("unlock tables")


if __name__ == "__main__":
    main()
