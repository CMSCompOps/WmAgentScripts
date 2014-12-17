import urllib2,urllib, httplib, sys, re, os
import optparse

parser = optparse.OptionParser()

parser.add_option('--correct_env',action="store_true",dest='correct_env')
parser.add_option('--node', help='Node',dest='node')
parser.add_option('--requestid', help='Request ID',dest='requestid')

(options,args) = parser.parse_args()

command=""
for arg in sys.argv:
    command=command+arg+" "

if options.node == None or options.requestid == None:
    print "must provide node name and request ID, exiting"
    sys.exit(1)

if not options.correct_env:
    os.system("source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh; python2.6 "+command + "--correct_env")
    sys.exit(0)

url='cmsweb.cern.ch'

conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))

requestid='428974'

params = urllib.urlencode({ "decision" : 'approve', "request" : options.requestid, "node": options.node, "comments" : 'auto-approval of relval dataset subscription'})
conn.request("POST", "/phedex/datasvc/json/prod/updaterequest", params)

response = conn.getresponse()

print response.status
print response.reason
