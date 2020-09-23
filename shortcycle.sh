BASE_DIR=/data/unified/WmAgentScripts/
# shellcheck disable=SC2034
HTML_DIR=/var/www/html/unified/

# shellcheck disable=SC2128
lock_name=$(echo "$BASH_SOURCE" | cut -f 1 -d ".").lock
# shellcheck disable=SC1090
source $BASE_DIR/cycle_common.sh "$lock_name"

##mapping the sites
$BASE_DIR/cWrap.sh Unified/mappor.py

## equalize site white list at the condor level                                                                                      
$BASE_DIR/cWrap.sh Unified/equalizor.py

## perform some alternative adhoc operations
$BASE_DIR/cWrap.sh Unified/addHoc.py


rm -f "$lock_name"

