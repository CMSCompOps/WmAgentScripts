BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

$BASE_DIR/cWrap.sh Unified/completor.py

rm -f $lock_name

