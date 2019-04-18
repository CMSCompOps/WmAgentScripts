BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name="$BASE_DIR/assigncycle.lock"

oweek=`date +%W`
week=${oweek#0}
let oddity=week%2

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
	cat $lock_name | mail -s "[Ops] Emergency On Cycle Lock. Unified isn't running." matteoc@fnal.gov,thong.nguyen@cern.ch,sharad.agarwal@cern.ch
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
    echo "Cannot read simple files" | mail -s "[Ops] read permission" matteoc@fnal.gov,thong.nguyen@cern.ch,sharad.agarwal@cern.ch
    exit
fi

echo $lock_name > $lock_name
echo `date` >> $lock_name
echo $$ >> $lock_name
## get sso cookie and new grid proxy
source $BASE_DIR/credentials.sh

## get the workflow in/out of the system
$BASE_DIR/cWrap.sh Unified/injector.py

## get the batches of relval annnounced to HN
$BASE_DIR/cWrap.sh Unified/batchor.py

## this could replace stagor in much faster
## needs to put some work in stagor or not ?
$BASE_DIR/cWrap.sh Unified/assignor.py --early --limit 10

#$BASE_DIR/cWrap.sh Unified/assignor.py --from staging 

## get the workflow in/out the system
$BASE_DIR/cWrap.sh Unified/injector.py

## assigned those that could have passed through directly
$BASE_DIR/cWrap.sh Unified/assignor.py --from_status staged

$BASE_DIR/cWrap.sh Unified/assignor.py _PR_newco
$BASE_DIR/cWrap.sh Unified/assignor.py _PR_ref

## assign the workflow to sites
$BASE_DIR/cWrap.sh Unified/assignor.py


rm -f $lock_name

