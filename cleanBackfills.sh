cd /data/unified/WmAgentScripts

oweek=`date +%W`
week=${oweek#0}
let oddity=week%2

export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

python cleanBackfills.py
