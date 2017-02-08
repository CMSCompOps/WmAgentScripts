#!/usr/bin/env python
from utils import getDatasetPresence, getDatasetEventsAndLumis
import json
import sys

url='cmsweb.cern.ch'

ev,lumi = getDatasetEventsAndLumis( sys.argv[1] )
if lumi:
    print ev, lumi, ev/float(lumi)
presence = getDatasetPresence(url, sys.argv[1])
print json.dumps( presence, indent=2)
