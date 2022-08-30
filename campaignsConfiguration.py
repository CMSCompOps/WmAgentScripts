#!/usr/bin/env python
from utils import mongo_client
import argparse
import ssl,pymongo
import json
import sys
from copy import deepcopy
from campaignAPI import updateCampaignConfig, deleteCampaignConfig, parseMongoCampaigns


def parseArgs():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Script to manage Campaign configurations")
    parser.add_argument('-d', '--dump', default=None, help="dump the whole content in this file")
    parser.add_argument('-l', '--load', default=None, help="synchronize the db with the content of this file")
    parser.add_argument('-n', '--name', help="a campaign name to be viewed/added/updated")
    parser.add_argument('-r', '--remove', default=False, action='store_true', help="remove the specified campaign")
    parser.add_argument('-c','--configuration',
                        help='either a json doc or a json string for the campaign to be add/updated')
    parser.add_argument('-p','--parameter',
                        help='a single parameter to be updated of the form key:value or a.b.key:value for nested')
    parser.add_argument('-t', '--type', default=None, choices=['relval'],
                        help='set only to relval for adding a relval campaign')
    args = parser.parse_args()
    return args


def main():
    """
    Execute the whole logic for campaign configuration management
    """
    options = parseArgs()

    client = mongo_client()
    db = client.unified.campaignsConfiguration

    if options.load:
        campaigns = []
        content = json.loads( open(options.load).read())
        for k,v in content.items():
            up = {'name' : k}
            v['name'] = k
            if options.type: v['type'] = options.type
            db.replace_one( up, v)
            campaigns.append(v)
            print k,v

            # Create the campaign if it doesn't exist
            found = db.find_one({"name": v})

            if not found:
                db.insert_one(up)
                createCampaign(up)
                print "Following campaign couldn't be found in the database. Inserting it"
                print up

        replaceCampaigns(campaigns)
        sys.exit(0)

    if options.dump:
        uc = {}
        for content in db.find():
            i=content.pop("_id")
            if content.get('type',None) != options.type: continue ## no relval
            if 'name' not in content:
                db.delete_one({'_id': i})
                print "dropping",i,content,"because it is malformated"
                continue
            uc[content.pop("name")] = content
        print len(uc.keys()),"campaigns damp"
        open(options.dump,'w').write(json.dumps( uc, indent =2, sort_keys=True))
        sys.exit(0)

    if options.remove:
        if options.name:
            db.delete_one({'name' : options.name})
            # and delete it in central couch too
            deleteCampaignConfig(options.name)
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
            ### Alan: can I assume options.name and options.configuration
            # contain the same campaign configuration?!?!
            replaceCampaigns(post)
        elif update:
            ## need to update a value
            if options.type: update['type'] = options.type
            print "updating",options.name,"with values",update
            db.update( up, {"$set": update} )
            ### And update it in central CouchDB as well
            thisDoc = deepcopy(found)
            thisDoc.update(update)
            replaceCampaigns(thisDoc)
        else:
            ## use that to show the value in the database
            # not other headers in the output, so that it can be json loadable
            found.pop('name')
            found.pop('_id')
            print json.dumps(found, indent=2, sort_keys=True)
    else:
        if post:
            ## entering a new value
            if options.type: post['type'] = options.type
            post.update( {"name":options.name})
            db.insert_one( post )
            createCampaign(post)
        elif update:
            if options.type: update['type'] = options.type
            update.update( {"name":options.name})
            db.insert_one( update )
            createCampaign(post)
        else:
            availables = [o["name"] for o in db.find()]
            print options.name," Not found. ",len(availables),"available campaigns \n","\n\t".join( sorted( availables))


def replaceCampaigns(campaigns):
    """
    If we replaced (updated) all the campaign records in MongoDB,
    let's do the same with CouchDB campaigns
    :param campaigns: list of campaign dictionaries
    """
    data = parseMongoCampaigns(campaigns, verbose=False)
    for rec in data:
        campName = rec['CampaignName']
        if not updateCampaignConfig(rec):
            print("FAILED to update campaign: %s. Full content was: %s" % (campName, rec))
        else:
            print("Campaign '%s' successfully updated in central CouchDB" % campName)


def createCampaign(content):
    """
    Parse the Unified campaign configuration and create a new
    campaign in central Couch
    :param content: the campaign content
    """
    data = parseMongoCampaigns(content, verbose=False)
    for rec in data:
        campName = rec['CampaignName']
        if not updateCampaignConfig(rec):
            print("FAILED to create campaign: %s. Full content was: %s" % (campName, rec))
        else:
            print("Campaign '%s' successfully created in central CouchDB" % campName)


if __name__ == '__main__':
    sys.exit(main())