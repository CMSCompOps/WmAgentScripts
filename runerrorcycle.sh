BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/var/www/html/unified/

lock_name=`echo $BASH_SOURCE | cut -f 1 -d "."`.lock
source $BASE_DIR/cycle_common.sh $lock_name

$BASE_DIR/cWrap.sh Unified/showError.py --ongoing --expose 2 --threads 2 --log_threads 2 

rm -f $lock_name

