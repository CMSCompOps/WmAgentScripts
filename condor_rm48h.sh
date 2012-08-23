#!/bin/sh

source ~cmst1/.bashrc
agentenv
tmp=`mktemp`
condor_overview | sed -n '/48 hours by workflow/,/-----/p' | grep " : " >> $tmp

while read line ; do

    WF=`echo "$line" | awk -F' : ' '{print $1}'`
    Jobs=`echo "$line" | awk -F' : ' '{print $2}'`
    echo "Checking : " $WF
    nRem=0
    for iJob in $Jobs ; do
       nStarts=`condor_q -l $iJob | grep NumJobStarts | awk -F' = ' '{print $2}'`
       if [[ $nStarts -gt 1 ]]; then
         condor_rm $iJob 
         nRem=`expr $nRem + 1` 
       fi 
    done
    echo " ---> "$nRem" Jobs Removed"

done < "$tmp"

rm $tmp

tmp=`mktemp`
condor_overview | sed -n '/Jobs with RemoveReason!=UNDEFINED/,/-----/p' | grep " : " >> $tmp

while read line ; do

    WF=`echo "$line" | awk -F' : ' '{print $1}'`
    Jobs=`echo "$line" | awk -F' : ' '{print $2}'`
    echo "Checking : " $WF
    nRem=0
    for iJob in $Jobs ; do   
      condor_rm -forcex $iJob
      nRem=`expr $nRem + 1`
    done
done < "$tmp"

rm $tmp
