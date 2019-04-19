BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## look at what is still running : add clear to expedite
$BASE_DIR/cWrap.sh Unified/checkor.py  --update --threads 5

rm -f $lock_name

