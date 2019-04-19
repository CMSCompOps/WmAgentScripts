BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## get sso cookie and new grid proxy
source $BASE_DIR/credentials.sh

## submit ACDCs and clones from actions submitted via new recovery tools
$BASE_DIR/cWrap.sh Unified/actor.py

rm -f $lock_name

