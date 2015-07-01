import reqMgrClient

import httplib
import json

import smtplib
import email

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate


import MySQLdb
import sys
import os
import time
import datetime

dbname = "relval"

import utils

import assignment

while True:

    mysqlconn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
    
    curs = mysqlconn.cursor()
    
    curs.execute("use "+dbname+";")

    #curs.execute("lock tables batches write, batches_archive write, workflows write, workflows_archive write, datasets write, clone_reinsert_requests write")

    curs.execute("select * from batches")
    batches=curs.fetchall()

    colnames = [desc[0] for desc in curs.description]
    
    for batch in batches:
        for name, value in zip(colnames, batch):
            print name+" => "+str(value)
            if name=="status":
                status=value
            elif name == "batch_id":
                batchid=value
            elif name == "site":
                site=value
            elif name == "processing_version":
                processing_version=value
            elif name == "hn_message_id":
                hn_message_id=value
            elif name == "announcement_title":
                title=value

        #if batch[0] == 48:
        #    continue

        #print batch
        print ""

        if status == "input_dsets_ready":
            
            print "assigning workflows in batch "+str(batchid)
            
            curs.execute("select workflow_name from workflows where batch_id = "+ str(batchid)+";")
            wfs=curs.fetchall()
            
            for wf in wfs:
                conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
                r1=conn.request("GET",'/reqmgr/reqMgr/request?requestName='+wf[0])
                r2=conn.getresponse()

                schema = json.loads(r2.read())

                for key, value in schema.items():
                    if type(value) is dict and key.startswith("Task"):
                        if ('KeepOutput' in value and value['KeepOutput']) or 'KeepOutput' not in value:
                            if 'InputDataset' in value:
                                dset="/" + value['InputDataset'].split('/')[1] + "/" + value['AcquisitionEra'] + "-" + value['ProcessingString'] + "-v" + str(processing_version)+"/*"

                                curs.execute("select * from datasets where dset_name = \""+ dset.rstrip("*")+"\";")

                                dbs_dset_check=utils.getDatasets(dset)
                                
                                if len(curs.fetchall()) != 0:
                                    os.system('echo '+wf[0]+' | mail -s \"assignment_loop.py error 1\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                elif len(dbs_dset_check) != 0:    
                                    os.system('echo '+wf[0]+' | mail -s \"assignment_loop.py error 5\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                else:   
                                    curs.execute("insert into datasets set dset_name=\""+dset.rstrip("*")+"\", workflow_name=\""+wf[0]+"\", batch_id="+str(batchid)+";")

                                    
                            elif 'PrimaryDataset' in value:

                                dset="/" + value['PrimaryDataset'] + "/" + value['AcquisitionEra'] + "-" + value['ProcessingString'] + "-v" + str(processing_version)+"/*"
                                curs.execute("select * from datasets where dset_name = \""+ dset.rstrip("*")+"\";")

                                curs_fetchall = curs.fetchall()

                                dbs_dset_check=utils.getDatasets(dset)

                                if len(curs_fetchall) != 0:
                                    os.system('echo '+wf[0]+" "+curs_fetchall[0][1]+" "+dset+' | mail -s \"assignment_loop.py error 2\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                elif len(dbs_dset_check) != 0:    
                                    os.system('echo '+wf[0]+' | mail -s \"assignment_loop.py error 7\" andrew.m.levin@vanderbilt.edu')
                                    sys.exit(1)
                                else:
                                    curs.execute("insert into datasets set dset_name=\""+dset.rstrip("*")+"\", workflow_name=\""+wf[0]+"\", batch_id="+str(batchid)+";")


                params = assignment.make_assignment_params(schema,site,processing_version)                    

                result=reqMgrClient.assignWorkflow("cmsweb.cern.ch", wf[0], "relval", params)

                if result != True:
                    os.system('echo '+wf[0]+' | mail -s \"assignment_loop.py error 4\" andrew.m.levin@vanderbilt.edu')
                    sys.exit(0)

                time.sleep(30)


            curs.execute("update batches set status=\"assigned\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")    

            mysqlconn.commit()

            if hn_message_id != "do_not_send_an_acknowledgement_email":

                msg = MIMEMultipart()
                reply_to = []
                send_to = ["andrew.m.levin@vanderbilt.edu"]
                #send_to = ["hn-cms-dataopsrequests@cern.ch","andrew.m.levin@vanderbilt.edu"]
                #send_to = ["hn-cms-hnTest@cern.ch"]

                msg['In-Reply-To'] = hn_message_id
                msg['References'] = hn_message_id

                msg['From'] = "amlevin@mit.edu"
                msg['reply-to'] = COMMASPACE.join(reply_to)
                msg['To'] = COMMASPACE.join(send_to)
                msg['Date'] = formatdate(localtime=True)
                msg['Subject'] = title
                msg['Message-ID'] = email.Utils.make_msgid()
    

                messageText="Dear all,\n"
                messageText=messageText+"\n"
                messageText=messageText+"This batch has been assigned.\n"
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


    #curs.execute("unlock tables")

    time.sleep(100)
