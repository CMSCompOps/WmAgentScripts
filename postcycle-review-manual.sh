BASE_DIR=/data/unified/WmAgentScripts/
# shellcheck disable=SC2034
HTML_DIR=/var/www/html/unified/

## export RUCIO_HOME
export RUCIO_HOME=~/.local/

# shellcheck disable=SC2128
lock_name=$(echo "$BASH_SOURCE" | cut -f 1 -d ".").lock
# shellcheck disable=SC1090
source $BASE_DIR/cycle_common.sh "$lock_name"

## look at everything that has to be taken care of : add clear to expedite 
$BASE_DIR/cWrap.sh Unified/checkor.py  --review --manual --threads 5

rm -f "$lock_name"

