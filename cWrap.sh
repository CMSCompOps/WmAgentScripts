BASE_DIR=/data/unified/WmAgentScripts/

HTML_DIR=/data/unified/www
FINAL_HTML_DIR=/eos/cms/store/unified/www/

if [ ! -d $FINAL_HTML_DIR ] ; then 
    echo "Cannot read the log destination",$FINAL_HTML_DIR
    exit
fi
cd $BASE_DIR

modulename=`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`
mkdir -p $HTML_DIR/logs/$modulename/
mkdir -p $FINAL_HTML_DIR/logs/$modulename/

last_log=$HTML_DIR/logs/$modulename/last.log
s_dated_log=$modulename/`date +%F_%T`.log
dated_log=$HTML_DIR/logs/$s_dated_log
log=$dated_log

echo `date` > $log
echo $$ >> $log

if [ -r unified_drain ] ; then
    echo "System is locally draining" >> $log
    cp $log $last_log
    cp $log $FINAL_HTML_DIR/logs/$modulename/.
    cp $log $FINAL_HTML_DIR/logs/$modulename/last.log
    exit
fi
if [ -r /eos/cms/store/unified/unified_drain ] ; then
    echo "System is globally draining" >> $log
    cp $log $last_log
    cp $log $FINAL_HTML_DIR/logs/$modulename/.
    cp $log $FINAL_HTML_DIR/logs/$modulename/last.log
    exit
fi


echo $USER >> $log
echo $HOSTNAME >> $log
echo module $modulename>> $log 

source ./set.sh

echo >> $log

start=`date +%s`
python ssi.py $modulename $start

python $* &>> $log

if [ $? == 0 ]; then
    echo "finished" >> $log
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
    tail -20 $emaillog | mail -s "[Ops] module "$modulename" failed" cmsunified@cern.ch,thong@caltech.edu 
fi

stop=`date +%s`
python ssi.py $modulename $start $stop
echo `date` >> $log

#cp $log $dated_log
cp $log $last_log
cp $log $FINAL_HTML_DIR/logs/$modulename/.
cp $log $FINAL_HTML_DIR/logs/$modulename/last.log

#rm $log
