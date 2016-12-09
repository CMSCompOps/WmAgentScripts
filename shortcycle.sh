lock_name="shortcycle.lock"

if [ -r /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name ] ; then
    
    if [ `ps -e -f | grep Uni | grep -c -v grep` == "0" ] ; then
	echo "There isn't anything running, very suspicious"
	sleep 30
	if [ `ps -e -f | grep Uni | grep -c -v grep` == "0" ] ; then
	    cat /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name /afs/cern.ch/user/c/cmst2/www/unified/logs/last_running | mail -s "[Ops] Emergency On Cycle Lock. Unified isn't running." vlimant@cern.ch,matteoc@fnal.gov
	    rm -f /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name
	fi
    fi
    echo "cycle is locked"
    exit
fi

if [ ! -r /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh ] ; then
    echo "Cannot read simple files" | mail -s "[Ops] read permission" vlimant@cern.ch,matteoc@fnal.gov
    exit
fi

echo `date` > /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name

## get sso cookie and new grid proxy
source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh

## all these below can run as often as we want since they only read from db
## and for most we want them to run very often

## equalize site white list at the condor level                                                                                      
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/equalizor.py

## finsih subscribing output blocks
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py away
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py assistance*
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py done
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py close

## subscribe everything that is being produced or waiting around
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py wmagent

/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/GQ.py

rm -f /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name

