cd /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts

week=`date +%W`
let oddity=week%2
if ( [ "$USER" = "vlimant" ] && [ "$oddity" = "1" ] ) || ( [ "$USER" = "mcremone" ] && [ "$oddity" = "0" ] ) ; then
    echo no go for $USER on week $week
    return
fi

export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
cat $HOME/private/$USER.txt | voms-proxy-init -voms cms --valid 140:00 -pwstdin

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

log=/afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/`date +%F_%T`.log

echo `date` > $log
echo >> $log

python $* >> $log

cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/last.log
