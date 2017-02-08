
import collect_job_failure_information
import print_job_failure_information
import assistance_decision
import optparse
import sys
import os

parser = optparse.OptionParser()
parser.add_option('--correct_env',action="store_true",dest='correct_env')
(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "

if not options.correct_env:
    os.system("source /cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example.sh; export X509_USER_PROXY=/tmp/x509up_u13536; python2.6 "+command + "--correct_env")
    sys.exit(0)

f = open("delete_this.txt", 'r')

wf_list = []

for line in f:
    wf_list.append(line.rstrip('\n'))

failure_information=collect_job_failure_information.collect_job_failure_information(wf_list)
assistance_decision=assistance_decision.assistance_decision(failure_information)
[istherefailureinformation,failure_information_string]=print_job_failure_information.print_job_failure_information(failure_information)

#print failure_information

print "assistance_decision:"
print assistance_decision

print failure_information_string

