BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

##mapping the sites
$BASE_DIR/cWrap.sh Unified/mappor.py

## equalize site white list at the condor level                                                                                      
$BASE_DIR/cWrap.sh Unified/equalizor.py

## perform some alternative adhoc operations
$BASE_DIR/cWrap.sh Unified/addHoc.py


rm -f $lock_name

