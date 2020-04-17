BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## initiate data placements or pass wf along
$BASE_DIR/cWrap.sh Unified/transferor.py

## get this done once as it is quite slow and heavy, but we need the output json it could produce
## in for timing measurements
#$BASE_DIR/cWrap.sh Unified/stagor.py

rm -f $lock_name

