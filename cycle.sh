if [ -r cycle.lock ] ; then
    echo "cycle is locked"
    exit
fi

echo `date` > cycle.lock
## get sso cookie and new grid proxy
source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh

## get the workflow in/back-in the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py

## check on on-going data placements
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/stagor.py

## initiate data placements or pass wf along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/transferor.py

## assign the workflow to sites
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py

## force-complete wf according to rules ## tagging phase
# heavy /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/completor.py

## verify sanity of completed workflow and pass along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py

## initiate automatic recovery
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py

## close the wf 
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/closor.py

## finsih subscribing output blocks
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py done

## run injector here to put back replacements if any prior to unlocking below
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py

## unlock dataset that can be unlocked and set status along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py

## and display
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/htmlor.py

rm -f cycle.lock