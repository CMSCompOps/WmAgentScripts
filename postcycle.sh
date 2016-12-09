lock_name="postcycle.lock"
##lock_name="precycle.lock"

if [ -r /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name ] ; then
    
    if [ `ps -e -f | grep Uni | grep -c -v grep` == "0" ] ; then
	echo "There isn't anything running, very suspicious"
	sleep 30
	if [ `ps -e -f | grep Uni | grep -c -v grep` == "0" ] ; then
	    cat /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name /afs/cern.ch/user/c/cmst2/www/unified/logs/last_running | mail -s "[Ops] Emergency On Cycle Lock. Unified isn't running." vlimant@cern.ch,matteoc@fnal.gov,Dmytro.Kovalskyi@cern.ch 
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

## get the workflow in/out the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py

## force-complete wf according to rules ## tagging phase
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/completor.py

## check on the wf that have completed already
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py --strict
## initiate automatic recovery
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py

## pass along everything that has custodial already and should close
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py  --review
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py  --clear
## close the wf in closed-out
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/closor.py

/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py  --update

rm -f /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/$lock_name

