cd /data/unified/WmAgentScripts

oweek=`date +%W`
week=${oweek#0}
let oddity=week%2

if ( [ "$USER" = "vlimant" ] && [ "$oddity" = "0" ] ) || ( [ "$USER" = "mcremone" ] && [ "$oddity" = "1" ] ) ; then
    echo no go for $USER on week $week
    exit
fi


export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

python Unified/inputs_summary.py 200
python Unified/inputs_summary.py pertape
