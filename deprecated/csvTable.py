#!/usr/bin/env python

import httplib, json, csv, time, smtplib, os, traceback
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
from dbs.apis.dbsClient import DbsApi

mailingList = ['luis89@fnal.gov']
#mailingList = ['cms-comp-ops-workflow-team@cern.ch']

def main():
    
    # Get workflows data from the server
    url = 'cmsweb.cern.ch'
    print 'INFO: Getting workflows data from ' + url
    
    header = {'Content-type': 'application/json',
               'Accept': 'application/json'}
    
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                    key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET",
                 '/couchdb/wmdatamining/_design/WMDataMining/_view/workflow_summary',
                 headers= header)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    
    # CSV writer needs utf-8 data format
    myString = data.decode('utf-8')
    workflows = json.loads(myString)['rows']
    
    # Create the CSV table and json File
    filename = 'table.csv'
    jsonFile = 'table.json'
    params = 'excel'
    workflows_dict = {}
    print 'INFO: Creating workflows table: ' + filename + ' with dialect: ' + params
    print 'INFO: Creating workflows json table: ' + jsonFile 
    
    # Get missing request events from DBS input dataset information
    dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
    dbsApi = DbsApi(url = dbsUrl)
    
    for entry in workflows:
        info = entry['value']
        if info[9] and type(info[9][0]) == list:
            info[9] = [info[9][x][0] for x in xrange(0, len(info[9]))]
        workflow_dict = {
                          'Campaign' : info[0],
                          'Tier' : info[1],
                          'Task type' : info[2],
                          'Status' : info[3],
                          'Priority' : info[4],
                          'Requested events' : info[5],
                          '% Complete' : info[6],
                          'Completed events' : 0,
                          'Request date' : time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(info[7])),
                          'Processing dataset name' : '',
                          'Input Dataset' : info[8],
                          'Output Datasets' : info[9],
                          }
        if workflow_dict['Requested events'] == 0:
            events = 0
            datasetName = workflow_dict['Input Dataset']
            if datasetName != "":
                response = dbsApi.listFileSummaries(dataset= datasetName)
                if response:
                    events = response[0]['num_event']
            workflow_dict['Requested events'] = events
            
        workflow_dict['Completed events'] = int(int(workflow_dict['Requested events']) * int(workflow_dict['% Complete']) / 100)
        if workflow_dict['Output Datasets'] != []:
            proccesedDataset = workflow_dict['Output Datasets'][0].split('/')[2]
            proccesingName = proccesedDataset.split('-')[-2]
            workflow_dict['Processing dataset name'] = proccesingName
        
        # Add to main dictionary
        workflows_dict[entry['id']] = workflow_dict
    
    jsonfile = open(jsonFile,'w+')
    jsonfile.write(json.dumps(workflows_dict, sort_keys=True, indent=3))
    jsonfile.close()
    print 'INFO: json table created' 
    
    max_lenght = 1
    for workflow_name in workflows_dict.keys():
         wfData = workflows_dict[workflow_name]
         if len(wfData['Output Datasets']) > max_lenght:
             max_lenght = len(wfData['Output Datasets'])
    
    with open(filename, 'wb') as csvfile:
        csvwriter = csv.writer(csvfile, dialect = params)
        fisrtRow = ['Workflow', 'Campaign', 'Tier', 'Task type', 'Status', 'Priority', 
                            'Requested events', '% Complete', 'Completed events',
                            'Request date', 'Processing dataset name', 'Input Dataset']
        fisrtRow.extend(['Output Dataset ' + str(x) for x in xrange(1, max_lenght + 1)])
        csvwriter.writerow(fisrtRow)
        for workflow_name in workflows_dict.keys():
            wfData = workflows_dict[workflow_name]
            entry = [workflow_name, wfData['Campaign'], wfData['Tier'], wfData['Task type'], 
                     wfData['Status'], wfData['Priority'], wfData['Requested events'], 
                     wfData['% Complete'], wfData['Completed events'], wfData['Request date'], 
                     wfData['Processing dataset name'], wfData['Input Dataset']]
            entry.extend(wfData['Output Datasets'])
            csvwriter.writerow(entry)
    print 'INFO: csv table created'
    
    #print 'Sending mail to: ', mailingList
    #body_text = 'Hi,\n\nAttached please find the summary table of the current state of the workflows in the system,\n\n'
    #body_text += 'Thanks,\nLuis C\n\n'
    #send_mail('luis89@fnal.gov',
    #          mailingList,
    #          'Workflows Table',
    #          body_text,
    #          [filename])
    print 'INFO: Script ran successfully'

def getInputEvents(datasetName):
    """
    This uses DBS web interface instead dbs API
    I am not using this right now, it can be switched.
    """
    events = 0
    
    if datasetName == "":
        return events
    
    url = 'cmsweb.cern.ch'
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'),
                                    key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET",
                 '/dbs/prod/global/DBSReader/filesummaries?dataset='+datasetName)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    
    events = data[0]['num_event']
    
    return events

def send_mail(send_from, send_to, subject, text, files=[], server="localhost"):
    assert isinstance(send_to, list)
    assert isinstance(files, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

if __name__ == "__main__":
    try:
        main()
    except Exception, ex:
        mssg = 'There was an error while executing csvTable.py on cmst2 acrontab, please have a look to the following error:\n'
        mssg += 'Exception: '+str(ex)+'\n'
        mssg += 'Traceback: '+str(traceback.format_exc())
        send_mail('noreply@cern.ch',
                  mailingList,
                  'Error executing cvsTable.py on cmst2 acrontab',
                  mssg)
        print 'ERROR: The script was not executed successfully\n\n'
        print mssg

    