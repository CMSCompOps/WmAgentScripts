assistance_exit_codes = ['8021','8028',61304]

default_threshold = 0.05

merge_threshold=0.1

harvesting_threshold=0.5

def assistance_decision(job_failure_information):
    
    assistance=False

    for wf in wf_dicts:
        firsttime=True
        for task in wf['task_dict']:
            if 'CleanupUnmerged' not in task['task_name'] and 'LogCollect' not in task['task_name']:
                continue
            sum = 0
            for exitcode in assistance_exit_codes:
                if exitcode in task['failures'].keys():
                    sum+=task['failures'][exitcode]['number']

            if "HarvestMerged" in task['task_name']:
                if float(sum) / task['nfailurestot'] > harvesting_threshold:
                    assistance=True

            elif "Merge" in task['task_name']:
                if float(sum) / task['nfailurestot'] > merge_threshold:
                    assistance=True

            else:
                if float(sum) / task['nfailurestot'] > default_threshold:
                    asssistance=True

    return assistance
