lock_name="precycle.lock"

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

### all below should be run non concurrently because they touch the lock file at some point
##

## get the workflow in/out of the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## this could replace stagor in much faster
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --early
## get the workflow in/out the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## initiate data placements or pass wf along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/transferor.py
## assigned those that could have passed through directly
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --from_status staged

## get the workflow in/out of the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## this could replace stagor in much faster
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --early --from_status staging
## get the workflow in/out the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## initiate data placements or pass wf along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/transferor.py

/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --from_status staged

## get this done once as it is quite slow and heavy, but we need the output json it could produce
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/stagor.py
## assign the workflow to sites
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py

## unlock dataset that can be unlocked and set status along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py

## addhoc operations
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/addHoc.py

## and display
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/htmlor.py

rm -f /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name

