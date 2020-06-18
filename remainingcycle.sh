BASE_DIR=/data/unified/WmAgentScripts/
# shellcheck disable=SC2034
HTML_DIR=/var/www/html/unified/

# shellcheck disable=SC2128
lock_name=$(echo "$BASH_SOURCE" | cut -f 1 -d ".").lock
# shellcheck disable=SC1090
source $BASE_DIR/cycle_common.sh "$lock_name"

# shellcheck disable=SC1090
$BASE_DIR/cWrap.sh Unified/remainor.py -n 5

rm -f "$lock_name"

