cd /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts

oweek=`date +%W`
week=${oweek#0}
let oddity=week%2

if ( [ "$USER" = "vlimant" ] && [ "$oddity" = "0" ] ) || ( [ "$USER" = "mcremone" ] && [ "$oddity" = "1" ] ) ; then
    echo no go for $USER on week $week
    exit
fi
modulename=`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`
log=/afs/cern.ch/user/c/cmst2/www/unified/logs/$modulename/`date +%F_%T`.log

echo `date` > $log
echo $USER >> $log
echo the week $week oddity is $oddity >> $log
echo module $modulename>> $log 

export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

echo >> $log

start=`date +%s`
echo $modulename:`date` >> /afs/cern.ch/user/c/cmst2/www/unified/logs/running
echo $modulename:`date` > /afs/cern.ch/user/c/cmst2/www/unified/logs/last_running
python $* &>> $log

if [ $? == 0 ]; then
    echo "finished" >> $log
else
    echo "abnormal termination" >> $log
    mail -s "[Ops] module "$modulename" failed" -a $log vlimant@cern.ch,matteoc@fnal.gov 
fi

stop=`date +%s`
let stop=stop-start
echo $modulename:$start:$stop > /afs/cern.ch/user/c/cmst2/www/unified/logs/$modulename/`date +%s`.time
echo `date` >> $log

## copy log to lasts
cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/$modulename/last.log
cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/last.log
