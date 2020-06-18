BASE_DIR=/data/unified/WmAgentScripts/
# shellcheck disable=SC2034
HTML_DIR=/var/www/html/unified/

# shellcheck disable=SC2128
lock_name=$(echo "$BASH_SOURCE" | cut -f 1 -d ".").lock
# shellcheck disable=SC1090
source $BASE_DIR/cycle_common.sh "$lock_name"

## get the workflow in/out of the system
$BASE_DIR/cWrap.sh Unified/injector.py

## get the batches of relval annnounced to HN
$BASE_DIR/cWrap.sh Unified/batchor.py

## this could replace stagor in much faster
## needs to put some work in stagor or not ?
$BASE_DIR/cWrap.sh Unified/assignor.py --early

## get the workflow in/out the system
$BASE_DIR/cWrap.sh Unified/injector.py

## assigned those that could have passed through directly
$BASE_DIR/cWrap.sh Unified/assignor.py --from_status staged

$BASE_DIR/cWrap.sh Unified/assignor.py _PR_newco
$BASE_DIR/cWrap.sh Unified/assignor.py _PR_ref

## assign the workflow to sites
$BASE_DIR/cWrap.sh Unified/assignor.py

rm -f "$lock_name"

