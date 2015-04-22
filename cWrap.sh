cd /afs/cern.ch/user/c/cmst2/Unified/WmAgentScripts

export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
cat $HOME/private/$USER.txt | voms-proxy-init -voms cms --valid 140:00 -pwstdin

source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

python $*
