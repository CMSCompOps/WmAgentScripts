BASE_DIR=/data/unified/WmAgentScripts/
#HTML_DIR=/var/www/html/unified/
#HTML_DIR=/data/unified/www
HTML_DIR=/eos/project/c/cms-unified-logs/www/

cd $BASE_DIR

#oweek=`date +%W`
#week=${oweek#0}
#let oddity=week%2

if [ "$USER" != "vlimant" ] ; then
    echo "single user running from now on"
    exit
fi


modulename=`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`
mkdir -p $HTML_DIR/logs/$modulename/
log=$HTML_DIR/logs/$modulename/last.log
dated_log=$HTML_DIR/logs/$modulename/`date +%F_%T`.log
echo `date` > $log
echo $USER >> $log
#echo the week $week oddity is $oddity >> $log
echo module $modulename>> $log 

#export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
#export X509_USER_PROXY=/tmp/$USER-4hty64k793hj
echo $X509_USER_PROXY >>$log

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

echo >> $log

start=`date +%s`
echo $modulename:`date` >> $HTML_DIR/logs/running
echo $modulename:`date` > $HTML_DIR/logs/last_running
python $* &>> $log

if [ $? == 0 ]; then
    echo "finished" >> $log
else
    echo "abnormal termination" >> $log
    mail -s "[Ops] module "$modulename" failed" -a $log vlimant@cern.ch,matteoc@fnal.gov,thong.nguyen@cern.ch
fi

stop=`date +%s`
let stop=stop-start
echo $modulename:$start:$stop > $HTML_DIR/logs/$modulename/`date +%s`.time
echo `date` >> $log

cp $log $dated_log