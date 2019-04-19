BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## unlock dataset that can be unlocked and set status along
$BASE_DIR/cWrap.sh Unified/lockor.py

rm -f $lock_name

