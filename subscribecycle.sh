BASE_DIR=/data/unified/WmAgentScripts/
# shellcheck disable=SC2034
HTML_DIR=/var/www/html/unified/

# shellcheck disable=SC2128
lock_name=$(echo "$BASH_SOURCE" | cut -f 1 -d ".").lock
# shellcheck disable=SC1090
source $BASE_DIR/cycle_common.sh "$lock_name"

## finish subscribing output blocks
$BASE_DIR/cWrap.sh Unified/subscribor.py away
$BASE_DIR/cWrap.sh Unified/subscribor.py assistance*
# shellcheck disable=SC1010
$BASE_DIR/cWrap.sh Unified/subscribor.py done
$BASE_DIR/cWrap.sh Unified/subscribor.py close

## subscribe everything that is being produced or waiting around
$BASE_DIR/cWrap.sh Unified/subscribor.py wmagent

rm -f "$lock_name"

