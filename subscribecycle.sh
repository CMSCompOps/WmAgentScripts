BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## finsih subscribing output blocks
$BASE_DIR/cWrap.sh Unified/subscribor.py away
$BASE_DIR/cWrap.sh Unified/subscribor.py assistance*
$BASE_DIR/cWrap.sh Unified/subscribor.py done
$BASE_DIR/cWrap.sh Unified/subscribor.py close

## subscribe everything that is being produced or waiting around
$BASE_DIR/cWrap.sh Unified/subscribor.py wmagent

rm -f $lock_name

