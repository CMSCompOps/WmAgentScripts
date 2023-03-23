import optparse
from utils import workflowInfo
from assignSession import session, Workflow



def searcher(completionThreshold, unifiedStatus):


    wfs = session.query(Workflow).filter(Workflow.status.startswith('assistance')).all()
    requests = []

    for wf in wfs:

        wfi = workflowInfo("cmsweb.cern.ch", wf.name)
        if wfi.request["RequestType"] in ["TaskChain", "StepChain"]:
            if wf.status == unifiedStatus:

                completions = wfi.getCompletionFraction()
                isOkay = True
                for out in completions:
                    completion = completions[out]
                    if float(completion) < float(completionThreshold):
                        isOkay = False

                if isOkay:
                    requests.append(wf.name)

    print "There are " + str(len(requests)) + " requests in status " + str(unifiedStatus) + " whose completion is higher than " + str(completionThreshold)
    print "Workflows: "
    for r in requests:
        print r


def main():
    parser = optparse.OptionParser()
    parser.add_option('-u', '--url', help='Which server to communicate with', default='cmsweb.cern.ch', choices=['cmsweb.cern.ch', 'cmsweb-testbed.cern.ch'])
    parser.add_option('-c', '--completion', help='completion out of 1', default=0.9)
    parser.add_option('-s', '--unifiedStatus', help='unifiedStatus', default="assistance-manual-recovered")
    (options, args) = parser.parse_args()

    searcher(options.completion, options.unifiedStatus)


if __name__ == "__main__":

    main()
