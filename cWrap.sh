cd /afs/cern.ch/user/v/vlimant/scratch0/ops/jr_WmAgentScripts

export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
cat /afs/cern.ch/user/v/vlimant/private/JeanRoch.txt | voms-proxy-init -voms cms --valid 140:00 -pwstdin

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

python $*
