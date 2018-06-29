#!/usr/bin/env python
from optparse import OptionParser
from utils import setAgentDrain, reqmgr_url

if __name__ == "__main__":
    myOptParser = OptionParser()
    myOptParser.add_option("-w", "--website",
                           default=reqmgr_url,
                           help="The url at which to send agent configuration")
    myOptParser.add_option("-a", "--agent",
                           help="The name of the agent to set in drain/undrain")
    myOptParser.add_option("-u", "--undrain",
                           default=False,
                           action="store_true",
                           help="Undrain instead of setting drain")
    (options,args) = myOptParser.parse_args()

    d = not options.undrain
    ok = setAgentDrain(options.website,
                       options.agent,
                       drain = d)
    if not ok:
        print "was not able to set drain",d,"to agent",options.agent
