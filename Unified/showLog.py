#!/usr/bin/env python
from utils import searchLog
import sys

subject =None
if sys.argv>2:
    subject=sys.argv[2]

o = searchLog( sys.argv[1] , 
               #actor=subject,
               limit=1000)
texts=set()
print "#"*20+"meta data"+"#"*20
print o[0]['_source']['meta']
for i in reversed(o):
    if len(texts)>50: break
    if i['_source']['text'] in texts: continue
    print "-"*10,i['_source']['subject'],"-"*2,i['_source']['date'],"-"*10
    print i['_source']['text']
    texts.add( i['_source']['text'] )
    

sys.exit(1)

