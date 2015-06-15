import MySQLdb
import sys
import datetime
import optparse
import json
import urllib2,urllib, httplib, sys, re, os

parser = optparse.OptionParser()
#parser.add_option('--correct_env',action="store_true",dest='correct_env')
(options,args) = parser.parse_args()

#command=""
#for arg in sys.argv:
#    command=command+arg+" "

#if not options.correct_env:
#    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
#    sys.exit(0)

dbname = "relval"

conn = MySQLdb.connect(host='dbod-cmsrv1.cern.ch', user='relval', passwd="relval", port=5506)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

curs.execute("select * from clone_reinsert_requests;")
requests_rows=curs.fetchall()

colnames = [desc[0] for desc in curs.description]

for requests_row in requests_rows:
    print requests_row

    for name, value in zip(colnames, requests_row):
        if name == "batch_id":
            batchid=value
        elif name == "new_site":
            site=value
        elif name == "new_processing_version":
            proc_ver=value

    now=datetime.datetime.now()

    useridyear=now.strftime("%Y")
    useridmonth=now.strftime("%m")
    useridday=now.strftime("%d")

    #the batch id of the new batch should be 1 more than any existing batch id
    curs.execute("select MAX(useridnum) from batches where useridyear=\""+useridyear+"\" and useridmonth=\""+useridmonth+"\" and useridday=\""+useridday+"\";")
    max_user_num_batches=curs.fetchall()[0][0]
    curs.execute("select MAX(useridnum) from batches_archive where useridyear=\""+useridyear+"\" and useridmonth=\""+useridmonth+"\" and useridday=\""+useridday+"\";")

    max_user_num_batches_archive=curs.fetchall()[0][0]

    if max_user_num_batches == None and max_user_num_batches_archive == None:
        usernum=0
    elif max_user_num_batches == None and max_user_num_batches_archive != None:
        usernum=max_user_num_batches_archive+1
    elif max_user_num_batches != None and max_user_num_batches_archive == None:
        usernum=max_user_num_batches+1
    else:
        usernum=max(max_user_num_batches,max_user_num_batches_archive)+1


    useridnum=usernum            

    curs.execute("select * from workflows where batch_id = \""+ str(batchid)+"\";")
    workflows_rows=curs.fetchall()
    curs.execute("select * from workflows_archive where batch_id = \""+ str(batchid)+"\";")
    workflows_archive_rows=curs.fetchall()

    if not ((len(workflows_rows) == 0 and len(workflows_archive_rows) > 0) or (len(workflows_rows) > 0 and len(workflows_archive_rows) == 0)):
        print "not ((len(workflows_rows) == 0 and len(workflows_archive_rows) > 0) or (len(workflows_rows) > 0 and len(workflows_archive_rows) == 0)), exiting"
        sys.exit(1)

    if len(workflows_archive_rows) > 0:
        workflows_rows = workflows_archive_rows
    
    print "cloning the workflows in batch "+str(batchid)

    workflows=[]
    
    for workflow_row in workflows_rows:
        #print workflow_row
        workflow=workflow_row[1]
        return_string=os.popen("python2.6 resubmit.py "+workflow + " anlevin DATAOPS").read()
        print return_string
        workflows.append(return_string.split(' ')[len(return_string.split(' ')) - 1].rstrip('\n'))

    print "finished cloning the workflows in batch "+str(batchid)

    curs.execute("select * from batches where batch_id = \""+ str(batchid)+"\";")
    batches_rows=curs.fetchall()

    batchescolnames = [desc[0] for desc in curs.description]

    curs.execute("select * from batches_archive where batch_id = \""+ str(batchid)+"\";")
    batches_archive_rows=curs.fetchall()
    if len(batches_rows) + len(batches_archive_rows) != 1:
        print "number of batches with this batch id is not equal to 1, exiting"
        sys.exit(1)
    if len(batches_archive_rows)  == 1:
        batches_rows = batches_archive_rows

    for name, value in zip(batchescolnames, batches_rows[0]):
        if name == "DN":
            DN=value
        if name == "announcement_title":
            email_title=value
        if name == "description":
            description=value
        if name == "useridyear":
            olduseridyear=value
        if name == "useridmonth":
            olduseridmonth=value
        if name == "useridday":
            olduseridday=value
        if name == "useridnum":
            olduseridnum=value

    olduserid=olduseridyear+"_"+olduseridmonth+"_"+olduseridday+"_"+str(olduseridnum)

    for line in workflows:
        workflow = line.rstrip('\n')
        if workflow == "":
            print "empty line in the file, exiting"
            sys.exit(1)
        curs.execute("select workflow_name from workflows where workflow_name=\""+ workflow +"\";")
        if len(curs.fetchall()) > 0:
            print "workflow "+workflow+" was already inserted into the database, exiting"
            sys.exit(1)

    #the batch id of the new batch should be 1 more than any existing batch id
    curs.execute("select MAX(batch_id) from batches;")
    max_batchid_batches=curs.fetchall()[0][0]
    curs.execute("select MAX(batch_id) from batches_archive;")
    max_batchid_batches_archive=curs.fetchall()[0][0]

    if max_batchid_batches == None and max_batchid_batches_archive == None:
        batchid=0;
    elif max_batchid_batches == None and max_batchid_batches_archive != None:
        batchid=max_batchid_batches_archive+1
    elif max_batchid_batches != None and max_batchid_batches_archive == None:
        batchid=max_batchid_batches+1
    else:     
        batchid=max(max_batchid_batches,max_batchid_batches_archive)+1

    #sanity checks to make sure this is really a new batchid
    curs.execute("select batch_id from batches where batch_id="+ str(batchid) +";")
    if len(curs.fetchall()) > 0:
        print "batch_id "+str(batchid)+" was already inserted into the batches database, exiting"
        sys.exit(1)

    curs.execute("select batch_id from workflows where batch_id="+ str(batchid) +";")
    if len(curs.fetchall()) > 0:
        print "batch_id "+str(batchid)+" was already inserted into the workflows database, exiting"
        sys.exit(1)     

    f_index=0
    g_index=0

    #check that the workflow name contains only letters, numbers, '-' and '_' 
    for workflow in workflows:
        workflow=workflow.rstrip('\n')
        for c in workflow:
            if c != 'a' and c != 'b' and c != 'c' and c != 'd' and c != 'e' and c != 'f' and c != 'g' and c != 'h' and c != 'i' and c != 'j' and c != 'k' and c != 'l' and c != 'm' and c != 'n' and c != 'o' and c != 'p' and c != 'q' and c != 'r' and c != 's' and c != 't' and c != 'u' and c != 'v' and c != 'w' and c != 'x' and c != 'y' and c != 'z' and c != 'A' and c != 'B' and c != 'C' and c != 'D' and c != 'E' and c != 'F' and c != 'G' and c != 'H' and c != 'I' and c != 'J' and c != 'K' and c != 'L' and c != 'M' and c != 'N' and c != 'O' and c != 'P' and c != 'Q' and c != 'R' and c != 'S' and c != 'T' and c != 'U' and c != 'V' and c != 'W' and c != 'X' and c != 'Y' and c != 'Z' and c != '0' and c != '1' and c != '2' and c != '3' and c != '4' and c != '5' and c != '6' and c != '7' and c != '8' and c != '9' and c != '_' and c != '-':
                print "workflow "+workflow+" contains the character "+str(c)+" which is not allowed, exiting"
                sys.exit(0)          

    #check that no workflows are repeated in the file
    for line1 in workflows:
        workflow1 = line1.rstrip('\n')
        for line2 in workflows:
            if f_index == g_index:
                continue
                g_index=g_index+1
            workflow2 = line2.rstrip('\n')
            if workflow1 == workflow2:
                print "workflow "+ workflow1+" is repeated twice in the input file, exiting"
                sys.exit(1)
            g_index=g_index+1
        f_index=f_index+1
                
    print "creating a new batch with batch_id = "+str(batchid)

    description = description.rstrip('\n')+"\n\n(clone of batch "+olduserid+")"

    curs.execute("insert into batches set batch_id="+str(batchid)+", DN=\""+DN+"\", announcement_title=\""+email_title+"\", processing_version="+str(proc_ver)+", site=\""+site+"\", description=\""+description+"\", status=\"approved\", useridyear=\""+useridyear+"\", useridmonth=\""+useridmonth+"\", useridday=\""+useridday+"\", useridnum="+str(useridnum)+", hn_message_id=\"do_not_send_an_acknowledgement_email\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\"")


    conn.commit()

    for line in workflows:
        workflow = line.rstrip('\n')
        curs.execute("insert into workflows set batch_id="+str(batchid)+", workflow_name=\""+workflow+"\";")

    #batchid is assigned the new batch id now    
    curs.execute("delete from clone_reinsert_requests where batch_id="+str(requests_row[0])+";")

    conn.commit()
