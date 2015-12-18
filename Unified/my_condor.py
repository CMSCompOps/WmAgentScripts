import json
from go_condor import makeAds

config = json.loads(open('equalizor.json').read())  
makeAds( config )
