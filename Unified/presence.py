#!/usr/bin/env python
from utils import getDatasetPresence
import json
import sys

url='cmsweb.cern.ch'

presence = getDatasetPresence(url, sys.argv[1])
print json.dumps( presence, indent=2)
