from utils import mongo_db_url
import optparse
import ssl,pymongo
import json
import sys
from collections import OrderedDict

parser = optparse.OptionParser()
parser.add_option('--dump',help="dump the whole content in this file",default=None)
parser.add_option('--load',help="synchronize the db with the content of this file", default=None)
parser.add_option('-n','--name')
parser.add_option('-d','--description')
parser.add_option('-v','--value')
(options,args) = parser.parse_args()


client = pymongo.MongoClient('mongodb://%s/?ssl=true'%mongo_db_url,
                             ssl_cert_reqs=ssl.CERT_NONE)
db = client.unified.unifiedConfiguration

if options.load:
    content = json.loads( open(options.load).read())
    for k,v in content.items():
        up = {'name' : k}
        s = {"$set": v}
        db.update( up, s )
        print k,v
    sys.exit(0)

if options.dump:
    uc = {}
    for content in db.find():
        content.pop("_id")
        uc[content.pop("name")] = content
    ouc = OrderedDict()
    for c in sorted(uc.keys()):
        ouc[c] = uc[c]
    open(options.dump,'w').write(json.dumps( ouc, indent =2))
    sys.exit(0)


post = {}# "name":options.name}
if options.description:
    post.update({"description":options.description})
if options.value:
    post.update({"value" : json.loads(options.value)})

found = db.find_one({"name":options.name})
if found:
    if post:
        ## need to update a value
        up = {'_id':found['_id']}
        s = {"$set": post}
        print "updating",options.name,"with values",post
        db.update( up, s )
    else:
        ## use that to show the value in the database
        # not other headers in the output, so that it can be json loadable
        print json.dumps(found["value"], indent=2)
else:
    if post:
        ## entering a new value
        post.update( {"name":options.name})
        db.insert_one( post )
    else:
        availables = [o["name"] for o in db.find()]
        print options.name," Not found. Available parameters \n","\n\t".join( sorted( availables))

