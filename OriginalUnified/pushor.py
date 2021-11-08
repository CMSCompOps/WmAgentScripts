import os
import json
from utils import sendEmail

pushes = [
    ('campaigns.json','Campaigns synching'),
    #('campaigns.relval.json','More relvals'),## directly in shared eos
    ('unifiedConfiguration.json','Update unified configuration')
    ]

operate = False

## push local commits to master
check = os.popen('git diff origin/master master').read()
if check:
    print "There are commits to be pushed to repo"
    print check
    #os.system('git checkout master')
    #os.system('git push origin master')

for fn,label in pushes:
    try:
        c = json.loads( open(fn).read())
        print fn,"loads properly"
        check = os.popen('git diff %s'%fn).read()
        if check:
            print "there are changes to",fn
            print check
            ## there are some changes
            if operate:
                print "pushing to GH"
                os.system('git add %s'%fn)
                os.system('git commit -m "%s"'% label)
                os.system('git push origin master')
            else:
                ## do not push, just let me know
                print "not pushing this time around"
                sendEmail('pushor','There are changes in %s, as \n%s'%( fn, check))
        else:
            print "There are no apparent changes to %s"%fn
    except:
        sendEmail('pushor','%s does not laod and cannot be pushed to GH'% fn)
