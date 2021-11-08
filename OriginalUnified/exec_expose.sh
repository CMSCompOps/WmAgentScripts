wf=$1
p=$2
er=$3
loc=$4
dest=$5
short=$6

echo "Parameters passed to execution:" $wf $p $er $loc $dest $short

#sudo -u cmst1 /bin/bash --init-file ~cmst1/.bashrc $loc/WmAgentScripts/Unified/expose.sh $wf $p $er $dest $short
source $loc/WmAgentScripts/Unified/expose.sh $wf $p $er $dest $short