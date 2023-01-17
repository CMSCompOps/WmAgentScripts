BASE_DIR=/data/unifiedPy3-fast/WmAgentScripts
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## Set up the environment
source /data/unifiedPy3-fast/setEnv.sh

## submit ACDCs and clones from actions submitted via new recovery tools
$BASE_DIR/cWrap.sh Unified/completor.py

rm -f $lock_name