BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

## look at everything that has to be taken care of : add clear to expedite 
$BASE_DIR/cWrap.sh Unified/checkor.py  --review --manual --threads 5

rm -f $lock_name

