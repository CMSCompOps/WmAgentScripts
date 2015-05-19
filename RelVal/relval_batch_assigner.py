webdir='.'
port = 50000

import os, sys
from BaseHTTPServer import HTTPServer
#from SimpleHTTPServer import SimpleHTTPRequestHandler
from CGIHTTPServer import CGIHTTPRequestHandler

#import cgitb
#cgitb.enable(display=0, logdir=".")

if len(sys.argv) > 1: webdir = sys.argv[1]
if len(sys.argv) > 2: port = int(sys.argv[2])
print 'webdir "%s",port %s' % (webdir, port)

os.chdir(webdir)
srvaddr = ("",port)
#srvrobj = HTTPServer(srvaddr, SimpleHTTPRequestHandler)
srvrobj = HTTPServer(srvaddr, CGIHTTPRequestHandler)
srvrobj.serve_forever()
