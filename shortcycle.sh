lock_name="/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/shortcycle.lock"

oweek=`date +%W`
week=${oweek#0}
let oddity=week%2

if ( [ "$USER" = "vlimant" ] && [ "$oddity" = "0" ] ) || ( [ "$USER" = "mcremone" ] && [ "$oddity" = "1" ] ) ; then
    echo no go for $USER on week $week
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
	echo "The cycle is locked but $lock_id is not running"
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

/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/addHoc.py

#/afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/cWrap.sh Unified/lockor.py

rm -f $lock_name

