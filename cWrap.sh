cd /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts

#talk to mcm
cern-get-sso-cookie -u https://cms-pdmv.cern.ch/mcm/ -o ~/private/prod-cookie.txt --krb

week=`date +%W`
let oddity=week%2

if ( [ "$USER" = "vlimant" ] && [ "$oddity" = "0" ] ) || ( [ "$USER" = "mcremone" ] && [ "$oddity" = "1" ] ) ; then
    echo no go for $USER on week $week
    exit
fi

#talk to cmsweb
export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
cat $HOME/private/$USER.txt | voms-proxy-init -voms cms --valid 140:00 -pwstdin

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
    
log=/afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/`date +%F_%T`.log

echo `date` > $log
echo $USER >> $log
echo the week $week oddity is $oddity >> $log
echo module `echo $1 | sed 's/\.py//' | sed 's/Unified\///'`>> $log 
echo >> $log

## check cmsweb up-time
python checkcmsweb.py >> $log || {
    cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/last.log
    cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/last.log
    exit 
}
# not all are using mcm, so we should not stop from running
#python checkmcm.py >> $log || {
#    cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/last.log
#    cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/last.log
#    exit 
#}

python $* &>> $log

## copy log to lasts
cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/`echo $1 | sed 's/\.py//' | sed 's/Unified\///'`/last.log
cp $log /afs/cern.ch/user/c/cmst2/www/unified/logs/last.log
