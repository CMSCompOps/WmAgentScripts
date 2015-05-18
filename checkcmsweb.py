from utils import workflowInfo
import sys

try:
    wfi = workflowInfo('cmsweb.cern.ch','pdmvserv_task_B2G-RunIIWinter15wmLHE-00067__v1_T_150505_082426_497')
except:
    print "cmsweb.cern.ch unreachable"
    sys.exit(123)
