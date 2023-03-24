import os
import sys
import subprocess
import json
import argparse
from utils import workflowInfo

def getAMsFromQuery(query):
    """ Get assistance-manuals from a query """
    string = """curl -X POST -H "Content-Type: application/json" -d '{{"query": "{query}"}}' tni-test.cern.ch/search/search""".format(query=query)
    result = json.loads(os.popen(string).read())
    return result

def main():

    parser = argparse.ArgumentParser(description='Famous Submitter')
    parser.add_argument("-t", "--test", action="store_true", help="Doesn't submit recoveries")

    options = parser.parse_args()

    url = 'cmsweb.cern.ch'
    query = 'exitCode = -2 AND status = assistance-manual-recovered'
    result = getAMsFromQuery(query)
    wfs = list(result.keys())

    stuckReRecoWfs = []
    successRecoveries, failedRecoveries = [], []
    for wf in wfs:
        wfi = workflowInfo(url, wf)
        reqType = wfi.request['RequestType']

        # this only works on rereco
        if reqType.lower() != 'rereco': continue

        stuckReRecoWfs.append(wf)

        cmd = 'python recovery.py --assign "--site original --xrootd --lumisperjob 1" --w {} --go'.format(wf)
        if options.test:
            print(cmd)
        else:
            print(cmd)
            output = subprocess.check_output(cmd, shell=True).decode(sys.stdout.encoding).strip()

            if "0 submitted" in output.lower():
                failedRecoveries.append(wf)
            else:
                successRecoveries.append(wf)

        break

    print("successRecoveries", successRecoveries)
    print("failedRecoveries", failedRecoveries)

if __name__ == "__main__":
    main()
