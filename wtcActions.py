"""
use the vocms0275 machine with the x509_USER_PROXY and export PYTHONPATH=$PYTHONPATH:/usr/lib64/python2.7/site-packages
"""

#!/usr/bin/env python
from utils import wtcInfo
import optparse
import os
user = os.environ.get('USER')

parser = optparse.OptionParser()
parser.add_option('--action', choices=['hold','bypass','force'])
parser.add_option('--keyword')
parser.add_option('--reason', help="A message to put in notification to mcm request", default="")
parser.add_option('--pop',action='store_true')
(options,args) = parser.parse_args()

WI = wtcInfo()
if options.pop:
    WI.remove( options.keyword )
else:
    WI.add( action= options.action,
            keyword = options.keyword,
            user= user,
            reason = options.reason
    )

