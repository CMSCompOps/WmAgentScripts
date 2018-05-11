BASE_DIR=/data/unified/WmAgentScripts/

HTML_DIR=/data/unified/www
FINAL_HTML_DIR=/eos/cms/store/unified/www/

cd $BASE_DIR

if [ "$USER" != "vlimant" ] ; then
    echo "single user running from now on"
    exit
fi

if [ -r unified_drain ] ; then
    echo "draining the local process"
    exit
fi
if [ -r /eos/cms/store/unified/unified_drain ] ; then
    echo "draining the global process"
    exit
fi

modulename=`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`
mkdir -p $HTML_DIR/logs/$modulename/
mkdir -p $FINAL_HTML_DIR/logs/$modulename/

last_log=$HTML_DIR/logs/$modulename/last.log
dated_log=$HTML_DIR/logs/$modulename/`date +%F_%T`.log
log=$dated_log

echo `date` > $log

if [ -r /eos/cms/store/unified/drain_mode ] ; then
    echo "System is draining"
    echo "System is draining" >> $log
    cp $log $last_log
    cp $log $FINAL_HTML_DIR/logs/$modulename/.
    cp $log $FINAL_HTML_DIR/logs/$modulename/last.log
    exit
fi



echo $USER >> $log
echo $HOSTNAME >> $log
#echo the week $week oddity is $oddity >> $log
echo module $modulename>> $log 

echo $MCM_SSO_COOKIE >>$log
echo $X509_USER_PROXY >>$log

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

echo >> $log

start=`date +%s`
echo $modulename:`date` >> $FINAL_HTML_DIR/logs/running
echo $modulename:`date` > $FINAL_HTML_DIR/logs/last_running
python $* &>> $log

if [ $? == 0 ]; then
    echo "finished" >> $log
else
    echo "abnormal termination" >> $log
    mail -s "[Ops] module "$modulename" failed" -a $log vlimant@cern.ch,matteoc@fnal.gov,thong.nguyen@cern.ch,svenja.pflitsch@desy.de
fi

stop=`date +%s`
let stop=stop-start
echo $modulename:$start:$stop > $FINAL_HTML_DIR/logs/$modulename/`date +%s`.time
echo `date` >> $log

#cp $log $dated_log
cp $log $last_log
cp $log $FINAL_HTML_DIR/logs/$modulename/.
cp $log $FINAL_HTML_DIR/logs/$modulename/last.log

#rm $log
