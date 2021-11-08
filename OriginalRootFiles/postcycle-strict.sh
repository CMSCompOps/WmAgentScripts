BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

## export RUCIO_HOME
export RUCIO_HOME=~/.local/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## those that just completed
$BASE_DIR/cWrap.sh Unified/checkor.py --strict --clear --threads 5

rm -f $lock_name

