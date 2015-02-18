#!/usr/bin/python -u

import MySQLdb

#from datetime import datetime

import datetime

import sys,os

print "Content-type: text/html\n"

import cgi,sys
form = cgi.FieldStorage()

f = open("relval_batch_assigner_logs/log2.dat", 'a')

f.write(str(datetime.datetime.now())+": "+str(form)+"\n")
f.flush()

#print form['batch0'].value
batchids_approval_pairs = []

for i in form:
    batchids_approval_pairs.append([i.strip("batch"),form[i].value] ) 
    
print batchids_approval_pairs

dbname = "relval"

conn = MySQLdb.connect(host='dbod-altest1.cern.ch', user='relval', passwd="relval", port=5505)
#conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

for pair in batchids_approval_pairs:
    print pair[1]
    if pair[1] == "approve":
        curs.execute("update batches set status=\"approved\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(pair[0]) +";")
    elif pair[1] == "disapprove":
        curs.execute("update batches set status=\"disapproved\", current_status_start_time=\""+datetime.datetime.now().strftime("%y:%m:%d %H:%M:%S")+"\" where batch_id = "+str(batchid) +";")
    else:
        if pair[1] != "null":
            print "unknown approve command, exiting"
            sys.exit()

conn.commit()

curs.close()

conn.close()
            
sys.exit();

title=form["AnnouncementTitle"].value
wfs=form["ListOfWorkflows"].value.split('\n')
procver=form["ProcessingVersion"].value
site = form["Site"].value
if 'Test' in form:
    test = True
else:
    test = False
statistics_fname = form["StatisticsFilename"].value
hnpost = form["HypernewsPost"].value
description = form["Description"].value

print "Description: "+description
print "<br>"
print "Hypernews Post: "+hnpost
print "<br>"
print "StatisticsFilename: "+statistics_fname
print "<br>"
print "Announcement e-mail title: "+title
print "<br>"
print "Site: "+site
print "<br>"
print "Processing version: "+procver
print "<br>"
print "Workflows:"
print "<br>"
for wf in wfs:
    wf = wf.rstrip('\n')
    wf = wf.rstrip('\r')
    if wf.strip() == "":
        continue
    print wf
    print "<br>"
print "Test: "+str(test)
print "<br>"



