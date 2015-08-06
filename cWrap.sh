cd /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts

week=`date +%W`
let oddity=week%2

if ( [ "$USER" = "vlimant" ] && [ "$oddity" = "0" ] ) || ( [ "$USER" = "mcremone" ] && [ "$oddity" = "1" ] ) ; then
    echo no go for $USER on week $week
    exit
fi

log=/afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/`date +%F_%T`.log

echo `date` > $log
echo $USER >> $log
echo the week $week oddity is $oddity >> $log
echo module `echo $1 | sed 's/\.py//' | sed 's/Unified\///'`>> $log 

#done in a separate, less frequent cron
#source /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts/credentials.sh &>> $log

export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

echo >> $log

###echo "Shutting things down !" >> $log

#tlog=/tmp/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/`date +%F_%T`.log
#python $* &> $tlog
#cat $tlog >> $log
python $* &>> $log


echo "finished" >> $log
echo `date` >> $log

## copy log to lasts
cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/last.log
cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/last.log
