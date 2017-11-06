BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name="$BASE_DIR/precycle.lock"

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

### all below should be run non concurrently because they touch the lock file at some point
##x

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
## initiate data placements or pass wf along
$BASE_DIR/cWrap.sh Unified/transferor.py
## assigned those that could have passed through directly
#$BASE_DIR/cWrap.sh Unified/assignor.py --from_status staged

## get the workflow in/out of the system
#$BASE_DIR/cWrap.sh Unified/injector.py
## this could replace stagor in much faster
#$BASE_DIR/cWrap.sh Unified/assignor.py --early --from_status staging
## get the workflow in/out the system
#$BASE_DIR/cWrap.sh Unified/injector.py
## initiate data placements or pass wf along
#$BASE_DIR/cWrap.sh Unified/transferor.py

#$BASE_DIR/cWrap.sh Unified/assignor.py --from_status staged

## get this done once as it is quite slow and heavy, but we need the output json it could produce
### right now, that modules fucks up ... too slow
## in for timing measurements
$BASE_DIR/cWrap.sh Unified/stagor.py
## assign the workflow to sites
$BASE_DIR/cWrap.sh Unified/assignor.py

## unlock dataset that can be unlocked and set status along
$BASE_DIR/cWrap.sh Unified/lockor.py

## addhoc operations
#$BASE_DIR/cWrap.sh Unified/addHoc.py

rm -f $lock_name

