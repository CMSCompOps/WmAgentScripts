BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## get the workflow in/out of the system
#$BASE_DIR/cWrap.sh Unified/injector.py

## get the batches of relval annnounced to HN
$BASE_DIR/cWrap.sh Unified/batchor.py

## this could replace stagor in much faster
## needs to put some work in stagor or not ?
$BASE_DIR/cWrap.sh Unified/assignor.py --early --priority 110000

## get the workflow in/out the system
#$BASE_DIR/cWrap.sh Unified/injector.py

## assigned those that could have passed through directly
$BASE_DIR/cWrap.sh Unified/assignor.py --from_status staged --priority 110000

## assign the workflow to sites
$BASE_DIR/cWrap.sh Unified/assignor.py --priority 110000

rm -f $lock_name

