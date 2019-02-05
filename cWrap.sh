BASE_DIR=/data/unified/WmAgentScripts/

HTML_DIR=/data/unified/www
FINAL_HTML_DIR=/eos/cms/store/unified/www/

cd $BASE_DIR

if [ "$USER" != "vlimant" ] ; then
    echo "single user running from now on"
    exit
fi

modulename=`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`
mkdir -p $HTML_DIR/logs/$modulename/
mkdir -p $FINAL_HTML_DIR/logs/$modulename/

last_log=$HTML_DIR/logs/$modulename/last.log
dated_log=$HTML_DIR/logs/$modulename/`date +%F_%T`.log
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
#echo the week $week oddity is $oddity >> $log
echo module $modulename>> $log 

echo $MCM_SSO_COOKIE >>$log
echo $X509_USER_PROXY >>$log

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
# get pymongo
###the local python fucks up with os.system('cp whatever whatever_on_eos')
export PYTHONPATH=$PYTHONPATH:/usr/lib64/python2.7/site-packages


echo >> $log

start=`date +%s`
python ssi.py $modulename $start

python $* &>> $log

if [ $? == 0 ]; then
    echo "finished" >> $log
else
    echo "abnormal termination" >> $log
    mail -s "[Ops] module "$modulename" failed" -a $log matteoc@fnal.gov,thong.nguyen@cern.ch,svenja.pflitsch@desy.de
fi

stop=`date +%s`
python ssi.py $modulename $start $stop
echo `date` >> $log

#cp $log $dated_log
cp $log $last_log
cp $log $FINAL_HTML_DIR/logs/$modulename/.
cp $log $FINAL_HTML_DIR/logs/$modulename/last.log

#rm $log
