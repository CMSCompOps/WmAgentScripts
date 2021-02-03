#!/usr/bin/env python
import time, shutil
from closeOutWorkflows import *

"""
    Runs Closeout script generating the output to a Web Page.
    Useful for sharing close out script information
"""

outputfile = '/afs/cern.ch/user/j/jbadillo/www/closeout.html'
tempfile = '/afs/cern.ch/user/j/jbadillo/www/temp.html'

head = ('<html>\n'
        '<head>\n'
        '<link rel="stylesheet" type="text/css" href="style.css" />\n'
        '<script language="javascript" type="text/javascript" src="actb.js"></script><!-- External script -->\n'
        '<script language="javascript" type="text/javascript" src="tablefilter.js"></script>\n'
        '<title>Closeout script Output</title>'
        '</head>\n'
        '<body>\n'
        '<h2>Close-out Script Summary</h2>\n'
        '<hr></hr>')

foot = ('<p>Last update: %s CERN time</p>\n'
        '</body>\n'
        '</html>')

def closeOutReRecoWorkflowsWeb(url, workflows, output):
    """
    closes rereco workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        try:
            if 'RelVal' in wf:
                continue
            if 'TEST' in wf:
                continue
            
            #first validate if effectively is completed
            workflow = reqMgrClient.ReReco(wf)
            if workflow.status != 'completed':
                continue
            #closeout workflow, checking percentage equalst 100%
            result = validateClosingWorkflow(url, workflow, closePercentage=1.0, 
                checkEqual=True, checkDuplicates=False)
            printResult(result)
            printResultWeb(result, output)
            #if result['closeOutWorkflow']:
            # TODO 
            #    reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
            #populate the list without subs
            missingSubs = True
            for (ds,info) in result['datasets'].items():
                missingSubs &= info['missingSubs']
            #if all missing subscriptions, subscribe all
            if missingSubs:
                noSiteWorkflows.append(workflow)
        except Exception as e:
            print wf, e
    print '-'*180
    return noSiteWorkflows

def closeOutRedigiWorkflowsWeb(url, workflows, output):
    """
    Closes Redigi workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #first validate if effectively is completed
        workflow = reqMgrClient.ReDigi(wf)
        if workflow.status != 'completed':
            continue
        #if miniaod
        if 'miniaod' in workflow.name:
            #we don't check for custodial subscription
            result = validateClosingWorkflow(url, workflow, 0.95, checkPhedex=False)            
        else:
            #check dataset health, duplicates, subscription, etc.       
            result = validateClosingWorkflow(url, workflow, 0.95)
        printResult(result)
        printResultWeb(result, output)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)

    print '-'*180
    return noSiteWorkflows

def closeOutMonterCarloRequestsWeb(url, workflows, output, fromGen):
    """
    Closes either montecarlo or montecarlo from gen
    workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #get all info from ReqMgr
        if not fromGen:  
            workflow = reqMgrClient.MonteCarlo(wf, url)
        else:
            workflow = reqMgrClient.MonteCarloFromGen(wf, url)
        #validate if complete
        if workflow.status != 'completed':
            continue
        #skip montecarlos on a special queue
        if workflow.team == 'analysis':
            continue
        # validation for SMS montecarlos
        if 'SMS' in workflow.outputDatasets[0]:
            closePercentage= 1.00
        else:
            closePercentage = 0.95
        #check dataset health, duplicates, subscription, etc.       
        result = validateClosingWorkflow(url, workflow, closePercentage=closePercentage)           
        printResult(result)
        printResultWeb(result, output)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    #separation line
    print '-'*180
    return noSiteWorkflows

def closeOutStep0RequestsWeb(url, workflows, output):
    """
    Closes either montecarlo step0 requests
    """
    noSiteWorkflows = []
    for wf in workflows:
        #info from reqMgr
        workflow = reqMgrClient.StepZero(wf, url)
        #first validate if effectively is completed
        if workflow.status != 'completed':
            continue
        #skip montecarlos on a special queue
        if workflow.team == 'analysis':
            continue
        #check dataset health, duplicates, subscription, etc.       
        result = validateClosingWorkflow(url, workflow, checkLumiNumb=True)           
        printResult(result)
        printResultWeb(result, output)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    print '-'*180
    return noSiteWorkflows

def closeOutStoreResultsWorkflowsWeb(url, workflows, output):
    """
    Closeout StoreResults workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #info from reqMgr            
        workflow = reqMgrClient.StoreResults(wf, url)
        #first validate if effectively is completed
        if workflow.status != 'completed':
            continue
        #closeout workflow, checking percentage equalst 100%
        result = validateClosingWorkflow(url, workflow, closePercentage=1.0, 
            checkEqual=True, checkDuplicates=False, checkPhedex='any')
        printResult(result)
        printResultWeb(result, output)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    print '-'*180
    return noSiteWorkflows


def closeOutTaskChainWeb(url, workflows, output):
    """
    Closeout taskchained workflows
    """
    noSiteWorkflows = []
    for wf in workflows:
        #first validate if effectively is completed
        workflow = reqMgrClient.TaskChain(wf, url)
        if workflow.status != 'completed':
            continue
        result = validateClosingTaskChain(url, workflow)   
        printResult(result)
        printResultWeb(result, output)
        #if validation successful
        if result['closeOutWorkflow']:
            reqMgrClient.closeOutWorkflowCascade(url, workflow.name)
        #populate the list without subs
        missingSubs = True
        for (ds,info) in result['datasets'].items():
            missingSubs &= info['missingSubs']
        #if all missing subscriptions, subscribe all
        if missingSubs:
            noSiteWorkflows.append(workflow)
    print '-'*180
    return noSiteWorkflows

def printResultWeb(result, output):
    """
    Prints the result of analysing a workflow in web output
    """
    for dsname, ds in result['datasets'].items():
        row = '<tr><td><a name="%s">%s</a></td>'+('<td>%s</td>'*9)+'</tr>\n'
        output.write( row % (result["name"], result["name"], dsname,
            "%.3f"%(ds["percentage"]*100),
            "?" if ds["duplicate"] is None else ds["duplicate"],
            "?" if ds["correctLumis"] is None else ds["correctLumis"],
            ','.join(ds["phedexReqs"]) if ds["phedexReqs"] else str(ds["phedexReqs"]),
            "?" if ds["transPerc"] is None else str(int(ds["transPerc"]*100)),
            "/?/" if "dbsFiles" not in ds else str(ds["dbsFiles"]),
            "?" if "phedexFiles" not in ds else str(ds["phedexFiles"]),
            ds["closeOutDataset"]))

def printTableHeaderWeb(title, output):
    output.write('<h3>%s</h3>'%title)
    output.write('<table border=1 id="%s" width="100%%">\n'%title)
    #output.write('<tr><th colspan="8">%s</th></tr>\n'%title)
    output.write('<tr><th>Request</th><th>OutputDataSet</th><th>%Compl</th>'
                '<th>Dupl</th><th>CorrectLumis</th><th>Subscr</th><th>Tran</th>'
                '<th>dbsF</th><th>phdF</th><th>ClosOu</th></tr>\n')

def printTableFooterWeb(title, output):
    output.write('</table>')
    output.write('<script language="javascript" type="text/javascript">\n'
        'var tableF = {\n'
        'col_0: "none",\n'
        'col_1: "none",\n'
        'col_2: "none",\n'
        'col_3: "select",\n'
        'col_4: "none",\n'
        'col_5: "select",\n'
        'col_6: "none",\n'
        'col_7: "none",\n'
        'col_8: "none",\n'
        'col_9: "select",\n'
        '};\n'+
        'setFilterGrid("%s",0,tableF);\n'%title+
        '</script>\n')

def listWorkflowsWeb(workflows, output):
    listWorkflows(workflows)
    for wf in workflows:
        for ds in wf.outputDatasets:
            output.write('<tr><td>%s</td><td>%s</td></tr>\n'%(wf.name,ds))
    output.write('<tr><td></td></tr>\n')

def listSubscriptionsWeb(subs, output):
    listSubscriptions(subs)
    for ds, site in subs:
        output.write('<tr><td>%s</td><td>%s</td></tr>\n'%(ds,site))
    output.write('<tr><td></td></tr>\n')


def main():
    output = open(tempfile,'w')
    url = 'cmsweb.cern.ch'
    print "Gathering Requests"
    requests = getOverviewRequestsWMStats(url)
    print "Classifying Requests"
    workflowsCompleted = classifyCompletedRequests(url, requests)
    #header
    output.write(head)
    #print header
    print '-'*220
    print '| Request'+(' '*74)+'| OutputDataSet'+(' '*86)+'|%Compl|Dupl|Tran|Subscr|ClosOu|'
    print '-'*220
    
    printTableHeaderWeb('ReReco', output)
    noSiteWorkflows = closeOutReRecoWorkflowsWeb(url, workflowsCompleted['ReReco'], output)
    workflowsCompleted['NoSite-ReReco'] = noSiteWorkflows
    printTableFooterWeb('ReReco', output)
    
    """
    printTableHeaderWeb('ReDigi', output)
    noSiteWorkflows = closeOutRedigiWorkflowsWeb(url, workflowsCompleted['ReDigi'], output)
    workflowsCompleted['NoSite-ReDigi'] = noSiteWorkflows
    printTableFooterWeb('ReDigi', output)
    
    printTableHeaderWeb('MonteCarlo', output)
    noSiteWorkflows = closeOutMonterCarloRequestsWeb(url, workflowsCompleted['MonteCarlo'], output, fromGen=False)
    workflowsCompleted['NoSite-MonteCarlo'] = noSiteWorkflows
    printTableFooterWeb('MonteCarlo', output)
    
    printTableHeaderWeb('MonteCarloFromGEN', output)
    noSiteWorkflows = closeOutMonterCarloRequestsWeb(url, workflowsCompleted['MonteCarloFromGEN'], output, fromGen=True)
    workflowsCompleted['NoSite-MonteCarloFromGEN'] = noSiteWorkflows
    printTableFooterWeb('MonteCarloFromGEN', output)
    
    printTableHeaderWeb('TaskChain', output)
    noSiteWorkflows = closeOutTaskChainWeb(url, workflowsCompleted['TaskChain'], output)
    workflowsCompleted['NoSite-TaskChain'] = noSiteWorkflows
    printTableFooterWeb('TaskChain', output)
    
    printTableHeaderWeb('LHEStepZero', output)
    noSiteWorkflows = closeOutStep0RequestsWeb(url, workflowsCompleted['LHEStepZero'],output)
    workflowsCompleted['NoSite-LHEStepZero'] = noSiteWorkflows
    printTableFooterWeb('LHEStepZero', output)
    
    printTableHeaderWeb('StoreResults', output)
    noSiteWorkflows = closeOutStoreResultsWorkflowsWeb(url, workflowsCompleted['StoreResults'], output)
    workflowsCompleted['NoSite-StoreResults'] = noSiteWorkflows
    printTableFooterWeb('StoreResults', output)
    """
    
    print "MC Workflows for which couldn't find Custodial Tier1 Site"

    
    output.write("<hr></hr>"
                "<h3>Datasets without subscriptions</h3>")
    output.write("<table border=1> <tr><th>Workfow</th><th>dataset</th></tr>")
    
    listWorkflowsWeb(workflowsCompleted['NoSite-ReReco'], output)
    """
    listWorkflowsWeb(workflowsCompleted['NoSite-ReDigi'], output)

    listWorkflowsWeb(workflowsCompleted['NoSite-MonteCarlo'], output)
    output.write("<tr><th>dataset</th><th>Subscribed to</th></tr>")
    subs = makeSubscriptions(url, workflowsCompleted['NoSite-MonteCarlo'])
    listSubscriptionsWeb(subs, output)

    listWorkflowsWeb(workflowsCompleted['NoSite-MonteCarloFromGEN'], output)
    output.write("<tr><th>dataset</th><th>Subscribed to</th></tr>")
    subs = makeSubscriptions(url, workflowsCompleted['NoSite-MonteCarloFromGEN'])
    listSubscriptionsWeb(subs, output)
    
    listWorkflowsWeb(workflowsCompleted['NoSite-TaskChain'], output)
    output.write("<tr><th>dataset</th><th>Subscribed to</th></tr>")
    subs = makeSubscriptions(url, workflowsCompleted['NoSite-TaskChain'])
    listSubscriptionsWeb(subs, output)

    listWorkflowsWeb(workflowsCompleted['NoSite-LHEStepZero'], output)
    listWorkflowsWeb(workflowsCompleted['NoSite-StoreResults'], output)
    """
    output.write('</table>')
    output.write(foot%time.strftime("%c"))
    output.close()
    #copy temporal to definitive file, avoid unavailability when running
    shutil.copy(tempfile, outputfile)  
    sys.exit(0);

if __name__ == "__main__":
    main()

