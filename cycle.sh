if [ -r /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock ] ; then
    echo "cycle is locked"
    #mail -s "[Ops] cycle is locked" vlimant@cern.ch,matteoc@fnal.gov
    exit
fi

echo `date` > /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock
## get sso cookie and new grid proxy
source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh

## get the workflow in/back-in the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py

## equalize site white list at the condor level
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/equalizor.py

## early assignement with all requirements included 
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --early --limit 100


## verify sanity of completed workflow and pass along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py
## initiate automatic recovery
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py


## check on on-going data placements
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/stagor.py
## get the workflow in/back-in the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## initiate data placements or pass wf along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/transferor.py
## assign the workflow to sites
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py

## equalize site white list at the condor level
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/equalizor.py

## verify sanity of completed workflow and pass along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py
## initiate automatic recovery
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py

## close the wf 
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/closor.py

## force-complete wf according to rules ## tagging phase
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/completor.py

## finsih subscribing output blocks
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py done
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py close

## run injector here to put back replacements if any prior to unlocking below
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py

## unlock dataset that can be unlocked and set status along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py

## the addHoc operations
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/addHoc.py

## and display
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/htmlor.py

## pre-fetch and place datasets from McM needs
#/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/collector.py

rm -f /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cycle.lock

