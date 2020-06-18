cd /data/unified/WmAgentScripts||exit

oweek=$(date +%W)
week=${oweek#0}
# shellcheck disable=SC2219,SC2034
let oddity=week%2


export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
# shellcheck disable=SC1091
source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

python Unified/inputs_summary.py 200
python Unified/inputs_summary.py pertape
