if [ -r cycle.lock ] ; then
    echo "cycle is locked"
    exit
fi

echo `date` > cycle.lock
source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/stagor.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/transferor.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py away
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/checkor.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py assistance
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py close
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/recoveror.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/closor.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py done
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/subscribor.py
rm -f cycle.lock