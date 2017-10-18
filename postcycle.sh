BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name="$BASE_DIR/postcycle.lock"

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


if [ ! -r $BASE_DIR/credentials.sh ] ; then
    echo "Cannot read simple files" | mail -s "[Ops] read permission" vlimant@cern.ch,matteoc@fnal.gov
    exit
fi

echo $lock_name > $lock_name
echo `date` >> $lock_name
echo $$ >> $lock_name

## get sso cookie and new grid proxy
source $BASE_DIR/credentials.sh

## get the workflow in/out the system
$BASE_DIR/cWrap.sh Unified/injector.py

## force-complete wf according to rules ## tagging phase
$BASE_DIR/cWrap.sh Unified/completor.py

## check on the wf that have completed already
$BASE_DIR/cWrap.sh Unified/checkor.py --strict
$BASE_DIR/cWrap.sh Unified/actor.py
## initiate automatic recovery
$BASE_DIR/cWrap.sh Unified/recoveror.py

## submit ACDCs and clones from actions submitted via new recovery tools
$BASE_DIR/cWrap.sh Unified/actor.py

## pass along everything that has custodial already and should close
$BASE_DIR/cWrap.sh Unified/checkor.py  --review
$BASE_DIR/cWrap.sh Unified/actor.py
$BASE_DIR/cWrap.sh Unified/checkor.py  --clear
$BASE_DIR/cWrap.sh Unified/actor.py

## close the wf in closed-out
$BASE_DIR/cWrap.sh Unified/closor.py

$BASE_DIR/cWrap.sh Unified/checkor.py  --update
$BASE_DIR/cWrap.sh Unified/actor.py

## early announce what can be announced already
$BASE_DIR/cWrap.sh Unified/closor.py --announce

rm -f $lock_name

