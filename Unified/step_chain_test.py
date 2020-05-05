#!/usr/bin/env python
from utils import getWorkflows, getWorkflowById, getWorkLoad, componentInfo, sendEmail, workflowInfo, sendLog, reqmgr_url, getDatasetStatus, unifiedConfiguration, moduleLock, do_html_in_each_module, getDatasetFiles
import sys

def step_chain_test(url, wf):
    transform_keywords = None
    wfi = workflowInfo(url, wf)
    good_for_stepchain = wfi.isGoodToConvertToStepChain( keywords = transform_keywords, debug=True)
    print "%s: %s\n" %(wf, good_for_stepchain)

if __name__ == "__main__":
    url = reqmgr_url
    if len(sys.argv)==2:
        wfs = [ sys.argv[1] ]
    else:
        wfs = [
            'pdmvserv_task_TSG-Run3Winter20wmLHEGS-00017__v1_T_200408_150110_9739',
            'pdmvserv_task_B2G-RunIIFall17wmLHEGS-01908__v1_T_200423_040350_2140',
            'pdmvserv_task_B2G-RunIISummer15wmLHEGS-03734__v1_T_200414_195359_2458',
            'pdmvserv_task_SMP-RunIISummer15wmLHEGS-00391__v1_T_200417_104228_4225'
        ]
    for wf in wfs:
        step_chain_test(url,wf)
    
