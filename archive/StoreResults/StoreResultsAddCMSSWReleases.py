"""
This script updates the CMMSW releases available in the StoreResults Savannah form
"""
import re
from mechanize import Browser

def ReleaseSort(x,y):
    if x==y:
        return 0
    releaseX = x.split('_')
    releaseY = y.split('_')
    for i in range(1,4):
        if (int(releaseX[i])<int(releaseY[i])):
            return -1
        elif (int(releaseX[i])>int(releaseY[i])):
            return 1
        elif (int(releaseX[i])==int(releaseY[i]) and len(releaseX)>len(releaseY) and i==3):
            return -1
    return 1
  
release_page = "https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML"
savannah_login_page='https://savannah.cern.ch/account/login.php?uri=%2F'
savannah_page="https://savannah.cern.ch/task/admin/field_values.php?group_id=599&list_value=1&field=custom_sb2"

## Open a browser connection, then look for releases
br = Browser()
# HTTP Error 403: request disallowed by robots.txt
br.set_handle_robots(False)
response = br.open(release_page)
html_output = response.read()
# releases basic lexicon 
ReleaseRegEx = re.compile('<project\slabel="(CMSSW_[0-9]+_[0-9]+_[0-9]+_*[A-Z,a-z,0-9,_]*).*>')
# Filter releases with lexicon
release_list = ReleaseRegEx.findall(html_output)
#remove duplicated entries and sort entries in reverse order
release_list = list(set(release_list))
release_list.sort(cmp=ReleaseSort,reverse=True)

## Now move browser to Savannah
br.open(savannah_login_page)
# 'Search' form is form 0
# login form is form 1
br.select_form(nr=1)

#read password
f = open('/home/cmsdataops/storeResults/secrets/SR_robot.txt', 'r')
passwd=f.readline()
f.close()

username="resultsrobot"

# Login into Savannah
br['form_loginname']=username
br['form_pw']=passwd.replace('\n','')
br.submit()
response = br.open(savannah_page)

# Check to see if login was successful, if not exit
if not re.search('Logged in as ' + username, response.read()):
    print 'login unsuccessful, please check your username and password'
    exit(1) # exit the program

cmssw2rank = {}
rank = 10

for release in release_list:
    cmssw2rank[release] = rank
    rank += 10

## Loop overall releases in Savannah and check if there are valid
## Hide deprecated versions
for links in br.links(text_regex="CMSSW_*_*"): 
    if links.text not in release_list:
        br.follow_link(links)
        print "Hide deprecated version %s" % links.text
        br.select_form(nr=1)
        control = br.find_control("status")
        control.value = ['H']
        br.submit()

## Doing a loop in all releases
## Create entry in Savannah for releases. If it already exist, update the rank
for release in release_list:
    try:
        #First, look if this CMSSW release is already there
        br.follow_link(text=release, nr=0)
        rank = cmssw2rank[release]
        print("Release %s already exists. Changing rank to %s" %(release,rank))
        br.select_form(nr=1)
        br['order_id']= str(rank)
        br['status']=['A']
        br.submit()
        br.open(savannah_page)
    except:
        br.select_form(nr=1)
        br.form.set_all_readonly(False)
        rank = cmssw2rank[release]
        br['title']=release
        br['order_id']= str(rank)
        br['description']='Created by StoreResults robot'
	print "Entry created for %s with rank %s" %(release,rank)
	br.submit()
	br.open(savannah_page)
