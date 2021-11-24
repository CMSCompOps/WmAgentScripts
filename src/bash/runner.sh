# Define base directories
BASE_DIR=/data/unified/WmAgentScripts/
HTML_DIR=/data/unified/www
FINAL_HTML_DIR=/eos/cms/store/unified/www/py3logs

if [ ! -d $FINAL_HTML_DIR ] ; then
    echo "Cannot read the log destination",$FINAL_HTML_DIR
    exit
fi
cd $BASE_DIR

modulename=`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`
mkdir -p $HTML_DIR/logs/$modulename/
env EOS_MGM_URL=root://eoscms.cern.ch eos mkdir -p $FINAL_HTML_DIR/logs/$modulename/

last_log=$HTML_DIR/logs/$modulename/last.log
s_dated_log=$modulename/`date +%F_%T`.log
dated_log=$HTML_DIR/logs/$s_dated_log
log=$dated_log

echo `date` > $log
echo $USER >> $log
echo $HOSTNAME >> $log
echo module $modulename>> $log

# Run the module
python $* &>> $log

if [ $? == 0 ]; then
    echo "Finished successfully, halting" >> $log
else
    echo -e "\nAbnormal termination with exit code $?" >> $log
    top -n1  -o %MEM -c >> $log

    emaillog=$log.txt
    failed_pid=$!
    echo "Abnormal termination, check $log" > $emaillog
    echo "https://cms-unified.web.cern.ch/cms-unified/logs/$s_dated_log" >> $emaillog
    echo $failed_pid >> $emaillog
    echo $USER >> $emaillog
    echo $HOSTNAME >> $emaillog
    echo -e "module $modulename \n" >> $emaillog
    tail $log >> $emaillog
    cat $emaillog | mail -s "[Ops] module "$modulename" failed" cmsunified@cern.ch
fi

cp $log $last_log
env EOS_MGM_URL=root://eoscms.cern.ch eos cp $log $FINAL_HTML_DIR/logs/$modulename/.
env EOS_MGM_URL=root://eoscms.cern.ch eos cp $log $FINAL_HTML_DIR/logs/$modulename/last.log