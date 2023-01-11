#!/usr/bin/env python
import sys
import optparse
import os
from assignSession import *

parser = optparse.OptionParser()
parser.add_option('--workflow', help='Which workflow logs', default=None)
parser.add_option('--logfile', help='What log file in particular', default=None)
parser.add_option('--task', help='What task to look for', default=None)
parser.add_option('--get', help='Retrieve the log file from eos', default=False,action ='store_true')
parser.add_option('--local', help='Where to get the log file', default='/tmp/%s'%(os.getenv('USER')))
parser.add_option('--where', default=False,action ='store_true')
parser.add_option('--eos', default=False, action='store_true')

(options,args) = parser.parse_args()

eos='/usr/bin/eos'

logs = []
if options.logfile:
    if options.workflow:
        logs = session.query(LogRecord).filter(LogRecord.logfile == options.logfile).filter(LogRecord.workflow == options.workflow).all()
    else:
        logs = session.query(LogRecord).filter(LogRecord.logfile == options.logfile).all()
else:
    if options.workflow:
        if options.task:
            logs = session.query(LogRecord).filter(LogRecord.workflow == options.workflow).filter(LogRecord.task == options.task).all()
        else:
            logs = session.query(LogRecord).filter(LogRecord.workflow == options.workflow).all()

if not logs:
    print("nothing found")
    sys.exit(1)

for log in logs:
    if options.where:
        print("found",log.logfile,"in",log.path)
    else:
        print("found",log.logfile,"for",log.workflow,"task",log.task)
    if options.get:
        out_dest = ('%s/%s'%( options.local, log.path.split('/')[-1])).replace('//','/')
        if options.eos:
            com = ('%s cp %s %s'%( eos, log.path, out_dest)).replace('//','/')
        else:
            com = ('cp %s %s'%( log.path, out_dest)).replace('//','/')
        com = "cd %s ; tar xvf %s `tar tvf %s | grep %s | awk '{print $NF}'`"%( options.local,
                                                                                log.path,
                                                                                log.path,
                                                                                log.logfile)
        print(com)
        os.system( com )



