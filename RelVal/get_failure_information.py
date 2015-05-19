
import jobFailureInformation
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

[istherefailureinformation,failureinformation]=jobFailureInformation.getFailureInformation("brm/159.txt","delete_this_2.txt",False,False)

print failureinformation
