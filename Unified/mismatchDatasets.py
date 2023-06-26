#!/usr/bin/env python
from assignSession import *
from utils import componentInfo, sendEmail, setDatasetStatus, unifiedConfiguration, workflowInfo, siteInfo, sendLog, reqmgr_url, monitor_dir, moduleLock, userLock, global_SI, do_html_in_each_module, getWorkflows, closeoutInfo, batchInfo
from utils import ThreadHandler
import threading
import reqMgrClient
import json
import time
import sys
import os
from utils import getDatasetEventsAndLumis, campaignInfo, getDatasetPresence, getWorkflowByCampaign
from htmlor import htmlor
from collections import defaultdict
import reqMgrClient
import re
import copy
import random
import optparse
import sqlalchemy 
from JIRAClient import JIRAClient
from campaignAPI import deleteCampaignConfig

def main():
    url = reqmgr_url
    output_dataset_list = []
    
    fmismatch_list = session.query(Workflow).filter(Workflow.status.startswith('assistance-filemismatch')).all()
    for wf in fmismatch_list:
        wf_name = wf.name
        wfi = workflowInfo(url, wf_name)
        output_dataset_list = output_dataset_list + wfi.request['OutputDatasets']
        
    #print(output_dataset_list)
    for element in output_dataset_list:
        print(element)

if __name__ == "__main__":
    main()
