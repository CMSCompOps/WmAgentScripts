USER=$(whoami)
kinit $USER@CERN.CH
source /afs/cern.ch/project/gd/LCG-share/3.2.11-1/etc/profile.d/grid-env.sh
git pull
voms-proxy-init -voms cms