from assignSession import *
from utils import reqmgr_url, workflowInfo
from optparse import OptionParser
import sys
import getpass



if __name__ == "__main__":

    username = getpass.getuser()

    parser = OptionParser()
    parser.add_option("-w", "--workflow", dest="workflow", help="The workflow name to be inserted")
    parser.add_option('-f', '--file', help='Text file with a list of workflows', dest='file')
    parser.add_option("-s", "--status", dest="status", help="Unified status to set", default="away")

    (options, args) = parser.parse_args()
    url = reqmgr_url

    if not (options.workflow or options.file):
        sys.exit("Either workflow or file option should be given, exiting")

    wfs = []
    if options.file:
        wfs = [l.strip() for l in open(options.file) if l.strip()]
    if options.workflow:
        wfs.append(options.workflow)

    for w in wfs:

        if w in session.query(Workflow).filter(Workflow.name.contains(w)).all():
            print ("The following workflow is already in the oracleDB, skipping", w)
        else:
            # wmstatus is dummy for now, we'll correct it later in the code
            new_wf = Workflow(name=w, status=options.status, wm_status="new")
            session.add(new_wf)
            session.commit()

            # Correct the wmstatus
            wfi = workflowInfo(url, w)
            schema = wfi.getSchema()
            requestStatus = schema['RequestStatus']

            workflowObj = session.query(Workflow).filter(Workflow.name.contains(w)).all()[0]
            workflowObj.wm_status = requestStatus
            session.commit()

            print("The workflow is inserted into unified w/ the following fields")
            print("name: ", workflowObj.name)
            print("status: ", workflowObj.status)
            print("wm_status: ", workflowObj.wm_status)


