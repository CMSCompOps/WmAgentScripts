if [ -r cycle.lock ] ; then
    echo "cycle is locked"
    exit
fi

echo `date` > cycle.lock
source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh
## get the workflow in/back-in the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## check on on-going data placements
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/stagor.py
## initiate data placements or pass wf along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/transferor.py
## assign the workflow to sites
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py
## get on-going blocks under dataops
#/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py away
## force-complete wf according to rules
#/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/completor.py
## verify sanity of completed workflow and pass along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py
## finish subscribing output blocks
#/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py assistance
## finsih subscribing output blocks
#/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py close
## initiate automatic recovery
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py
## close the wf 
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/closor.py
## finsih subscribing output blocks
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py done
## unlock dataset that can be unlocked and set status along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py
## no use anymore, consider backlog taken care of
##/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py
rm -f cycle.lock