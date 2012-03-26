#!/bin/bash
MANAGEDIR="/data/srv/wmagent/current"

usage()
{
	echo "$0"
}

init_thres()
{
	SITE=$1
	$MANAGEDIR/config/wmagent/manage execute-agent wmagent-resource-control --site-name=$SITE --task-type=Merge --task-slots=100
	$MANAGEDIR/config/wmagent/manage execute-agent wmagent-resource-control --site-name=$SITE --task-type=LogCollect --task-slots=100
	$MANAGEDIR/config/wmagent/manage execute-agent wmagent-resource-control --site-name=$SITE --task-type=Cleanup --task-slots=100
}

echo -n "Retrieving site list..."
LIST=$(./config/wmagent/manage execute-agent wmagent-resource-control -p | awk '/^T/{print $1}')
echo $LIST
for i in $LIST; do
	init_thres $i
done
