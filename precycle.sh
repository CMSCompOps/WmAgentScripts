lock_name="/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/precycle.lock"

oweek=`date +%W`
week=${oweek#0}
let oddity=week%2

if ( [ "$USER" = "vlimant" ] && [ "$oddity" = "1" ] ) || ( [ "$USER" = "mcremone" ] && [ "$oddity" = "0" ] ) ; then
    echo no go for $USER on week $week
    exit
fi

if [ $USER == "mcremone" ] ; then 
    echo "Sorry Matteo, not now"
    exit
fi

if [ -r $lock_name ] ; then
    echo "lock file $lock_name is present"
    echo current id is $$
    lock_id=`tail -1 $lock_name`
    echo looking for $lock_id
    lock_running=`ps -e -f | grep " $lock_id " | grep -c -v grep`
    ps -e -f | grep " $lock_id " | grep -v grep
    echo $lock_running
    if [ "$lock_running" == "0" ] ; then
	echo "The cycle is locked but $lock_id is not running. Lifting the lock"
	ps -e -f | grep Unified
	cat $lock_name | mail -s "[Ops] Emergency On Cycle Lock. Unified isn't running." vlimant@cern.ch,matteoc@fnal.gov
	rm -f $lock_name
    else
	echo "cycle is locked"
	echo $lock_id,"is running"
	ps -e -f | grep Unified
	exit
    fi
else
    echo "no lock file $lock_name, cycle can run"
fi

if [ ! -r /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh ] ; then
    echo "Cannot read simple files" | mail -s "[Ops] read permission" vlimant@cern.ch,matteoc@fnal.gov
    exit
fi

echo $lock_name > $lock_name
echo `date` >> $lock_name
echo $$ >> $lock_name
## get sso cookie and new grid proxy
source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh

### all below should be run non concurrently because they touch the lock file at some point
##x

## get the workflow in/out of the system
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/injector.py
## this could replace stagor in much faster
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py --early --limit 200

/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py RunIISummer16DR80Premix --from staging --early

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
### right now, that modules fucks up ...
###/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/stagor.py
## assign the workflow to sites
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/assignor.py

## unlock dataset that can be unlocked and set status along
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py

## addhoc operations
/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/addHoc.py

rm -f $lock_name

