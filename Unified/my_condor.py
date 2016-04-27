import json
from utils import monitor_dir
from go_condor import makeAds

config = json.loads(open('%s/equalizor.json'%monitor_dir).read())  
makeAds( config )
