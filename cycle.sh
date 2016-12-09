if [ -r /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock ] ; then
    
    if [ `ps -e -f | grep Uni | grep -c -v grep` == "0" ] ; then
	echo "There isn't anything running, very suspicious"
	sleep 30
	if [ `ps -e -f | grep Uni | grep -c -v grep` == "0" ] ; then
	    cat /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock /afs/cern.ch/user/c/cmst2/www/unified/logs/last_running | mail -s "[Ops] Emergency On Cycle Lock. Unified isn't running." vlimant@cern.ch,matteoc@fnal.gov,Dmytro.Kovalskyi@cern.ch 
	    rm -f /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock
	fi
    fi
    echo "cycle is locked"
    exit
fi

if [ ! -r /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh ] ; then
    echo "Cannot read simple files" | mail -s "[Ops] read permission" vlimant@cern.ch,matteoc@fnal.gov
    exit
fi

echo `date` > /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock
## get sso cookie and new grid proxy
source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh

## get the workflow in/back-in the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py

## equalize site white list at the condor level
#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/equalizor.py

## early assignement with all requirements included 
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --early --limit 100
#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py away
#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py assistance*

## verify sanity of completed workflow and pass along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py --strict
## initiate automatic recovery
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py


## check on on-going data placements
## this could replace stagor in much faster
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --early --from_status staging
##/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/stagor.py

## get the workflow in/back-in the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## initiate data placements or pass wf along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/transferor.py
## assign the workflow to sites
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py

## equalize site white list at the condor level
#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/equalizor.py

## verify sanity of completed workflow and pass along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py 
## initiate automatic recovery
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py

## pass along everything that has custodial already and should close
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py  --clear
## close the wf 
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/closor.py

## force-complete wf according to rules ## tagging phase
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/completor.py

## finsih subscribing output blocks
#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py done
#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py close

## run injector here to put back replacements if any prior to unlocking below
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py

## unlock dataset that can be unlocked and set status along
short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py

## the addHoc operations
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/addHoc.py

## and display
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/htmlor.py

## subscribe everything that is being produced or waiting around
#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py wmagent

#short /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/GQ.py


rm -f /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock

