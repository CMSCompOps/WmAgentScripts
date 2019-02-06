#!/usr/bin/env python
from utils import mongo_client
import optparse
import ssl,pymongo
import json
import sys

parser = optparse.OptionParser()
parser.add_option('--dump',help="dump the whole content in this file",default=None)
parser.add_option('--load',help="synchronize the db with the content of this file", default=None)
parser.add_option('-n','--name', help='a campaign name to be viewed/added/updated')
parser.add_option('--remove', help="remove the specified campaign", default=False, action='store_true')
parser.add_option('-c','--configuration', help='either a json doc or a json string for the campaign to be add/updated')
parser.add_option('-p','--parameter', help='a single parameter to be updated of the form key:value or a.b.key:value for nested')
parser.add_option('--type', default=None, help='set only to relval for adding a relval campaign', choices = ['relval'])
(options,args) = parser.parse_args()


client = mongo_client()
db = client.unified.campaignsConfiguration

if options.load:
    content = json.loads( open(options.load).read())
    for k,v in content.items():
        up = {'name' : k}
        #s = {"$set": v}
        #db.update( up, s )
        ## replace the db content
        v['name'] = k
        if options.type: v['type'] = options.type
        db.replace_one( up, v)

        print k,v
    sys.exit(0)

if options.dump:
    uc = {}
    for content in db.find():
        i=content.pop("_id")
        if content.get('type',None) != options.type: continue ## no relval
        if 'name' not in content:
            #db.delete_one({'_id': i})
            print "dropping",i,content,"because it is malformated"
            #continue
            pass
        uc[content.pop("name")] = content
    print len(uc.keys()),"campaigns damp"
    open(options.dump,'w').write(json.dumps( uc, indent =2))
    sys.exit(0)

if options.remove:
    if options.name:
        db.delete_one({'name' : options.name})
    else:
        pass
    sys.exit(0)

post = {}
if options.configuration:
    try:
        post.update(json.loads(options.configuration))
    except:
        post.update(json.loads(open(options.configuration).read()))
    post['name'] = options.name
update = {}
if options.parameter:
    name,value = options.parameter.split(':',1)
    ## convert to int or float or object
    try:
        value = int(value)
    except:
        try:
            value = float(value)
        except:
            try:
                value = json.loads(value)
            except:
                # as string
                pass


    if '.' in name:
        path = list(name.split('.'))
        w = update
        for p in path[:-1]:
            w[p] = {}
            w = w[p]
        w[path[-1]] = value
    else:
        update[name] = value
        

found = db.find_one({"name":options.name})
if found:
    up = {'_id':found['_id']}
    if post:
        print "replacing",options.name,"with values",post
        if options.type: post['type'] = options.type
        db.replace_one(up, post)
    elif update:
        ## need to update a value
        if options.type: update['type'] = options.type
        print "updating",options.name,"with values",update
        db.update( up, {"$set": update} )
    else:
        ## use that to show the value in the database
        # not other headers in the output, so that it can be json loadable
        found.pop('name')
        found.pop('_id')
        print json.dumps(found, indent=2)
else:
    if post:
        ## entering a new value
        if options.type: post['type'] = options.type
        post.update( {"name":options.name})
        db.insert_one( post )
    elif update:
        if options.type: update['type'] = options.type
        post.update( {"name":options.name})
        db.insert_one( update )        
    else:
        availables = [o["name"] for o in db.find()]
        print options.name," Not found. ",len(availables),"available campaigns \n","\n\t".join( sorted( availables))

