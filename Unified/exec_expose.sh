wf=$1
p=$2
er=$3
loc=$4
dest=$5
echo $wf $p $er $loc $dest

sudo -u cmst1 /bin/bash --init-file ~cmst1/.bashrc $loc/WmAgentScripts/Unified/expose.sh $wf $p $er $dest