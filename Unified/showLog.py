#!/usr/bin/env python
from utils import searchLog
import sys

o = searchLog( sys.argv[1] )
for i in reversed(o):
    print "-"*10,i['_source']['subject'],"-"*2,i['_source']['date'],"-"*10
    print i['_source']['text']

sys.exit(1)

