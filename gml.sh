#!/bin/sh
#
# 2012-06-21
# Global Monitor Light script

TMPDIR=/tmp/cmst1
treswarn=95
tresalarm=70

if [ -f ${TMPDIR}/slot-limits.conf ]; then rm ${TMPDIR}/slot-limits.conf; fi
wget -P ${TMPDIR} https://cmst1.web.cern.ch/CMST1/wmaconfig/slot-limits.conf 

echo "Getting global condor_overview..."
if [ -f ${TMPDIR}/condor_overview.txt ]; then rm ${TMPDIR}/condor_overview.txt; fi
`python /afs/cern.ch/user/c/cmst1/condor_global_overview.py > ${TMPDIR}/condor_overview.txt`
echo "Got it."

T2List=`(cat ~cmst1/scripts/T2List.txt | awk -F"|" '{print $2}' | sed "s: ::g")`

printf " %-20s | %-10s | %-10s | %-10s | %-10s | %-10s\n"   T2 Pending Running Slots Threshold Status
echo   "----------------------+------------+------------+------------+------------+------------+" 

check=false

for T2 in $T2List ; do
  if [ ! $check == true -a "${T2:0:2}" == "T2" ]; then echo "----------------------+------------+------------+------------+------------+------------+"; check=true; fi

  slots=`(cat ~cmst1/scripts/T2List.txt | grep $T2 | awk -F"|" '{print $4}' | sed "s: ::g")`
  if [ `echo $slots | grep ",5"` ]; then slots=${slots:0:$((${#slots}-2))}; fi

  Run=0
  cat ${TMPDIR}/condor_overview.txt | sed -n '/Running/,/Total/p' > ${TMPDIR}/condor_overview.tmp
  if (grep -q "$T2" ${TMPDIR}/condor_overview.tmp) 
  then
    AddRun=`(cat ${TMPDIR}/condor_overview.txt |  sed -n '/Running/,/Total/p' | grep $T2 | awk -F"|" '{print $8}' | sed "s: ::g")`
    for Add in $AddRun ; do  
      Run=`expr $Run + $Add`
    done
  fi
 
  Pend=0
  cat ${TMPDIR}/condor_overview.txt | sed -n '/Pending/,/Total/p' > ${TMPDIR}/condor_overview.tmp
  if (grep -q "$T2" ${TMPDIR}/condor_overview.tmp) 
  then
    AddPend=`(cat ${TMPDIR}/condor_overview.txt |  sed -n '/Pending/,/Total/p' | grep $T2 | awk -F"|" '{print $8}' | sed "s: ::g")`
    for Add in $AddPend ; do  
      Pend=`expr $Pend + $Add`
    done
  fi
  
  threshold=`(grep $T2 slot-limits.conf | awk '{print $2}')`
  status=`(grep $T2 slot-limits.conf | awk '{print $3}')`

  # colorizing
  oPend=$Pend
#  if [ "$(($slots*8/10))" -gt "$Run" ] && [ "$oPend" -gt "0" ]; then Pend="\033[1;34m$oPend\033[0;30m"; fi
  if [ "$oPend" == "0" ]; then Pend="\033[0;31m$oPend\033[0;30m"; fi

  oRun=$Run
  oT2=$T2
  if [ "$oRun" -lt $(($slots*$tresalarm/100)) -a "$oRun" -gt 0 -a "$oPend" -gt 0 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then Run="\033[1;31m$oRun\033[0;30m"; T2="\033[0;31m$oT2\033[0;30m"; listAlarm=(${listAlarm[@]}" $oT2 is running $oRun, $slots slots.\n"); fi
  if [ "$oRun" -lt $(($slots*$treswarn/100)) -a "$oRun" -gt $(($slots*tresalarm/100)) -a "$oPend" -gt 0 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then Run="\033[1;35m$oRun\033[0;30m"; T2="\033[0;35m$oT2\033[0;30m"; listWarn=(${listWarn[@]}" $oT2 is running $oRun, $slots slots.\n"); fi
  if [ "$oRun" == "0" ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then Run="\033[0;31m$oRun\033[0;30m"; T2="\033[0;31m$oT2\033[0;30m"; fi
  if [ "$oRun" == "0" ]; then Run="\033[0;31m$oRun\033[0;30m"; fi

  # making lists
  if [ "$oPend" -lt 10 ] && [ "$oRun" -lt 10 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then listEmpty=(${listEmpty[@]}" $oT2 is running $oRun, has $oPend pending, $slots slots --> empty\n"); fi
  if [ "$oRun" -lt "10" ]  && [ "$oPend" -gt 10 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then listGlideIn=(${listGlideIn[@]}" $oT2 is running $oRun, has $oPend pending, $slots slots --> GlideIn problem?\n"); fi
  if [ "$status" == "drain" ] || [ "$status" == "down" ]; then listStatus=(${listStatus[@]}" $oT2 status $status: still $oRun running, $oPend pending.\n"); fi

#  if [ "$status" == "drain" ]; then T2="\033[0;30m\033[0;43m$oT2\033[0m\033[0m"; fi
#  if [ "$status" == "down" ]; then T2="\033[0;30m\033[0;41m$oT2\033[0m\033[0m"; fi

  printf " %b%*s | %b%*s | %b%*s | %-10s | %-10s | %-10s    \n"   $T2 $((20-${#oT2})) "" $Pend $((10-${#oPend})) "" $Run $((10-${#oRun})) "" $slots $threshold $status

done

echo
echo
echo -e "\033[1;31mEmpty sites:\033[0;30m\n ${listEmpty[@]}"
echo
echo -e "\033[1;35mWarning sites ($tresalarm% < xx < $treswarn%):\033[0;30m\n ${listWarn[@]}"
echo
echo -e "\033[1;31mAlarm sites (xx < $tresalarm%):\033[0;30m\n ${listAlarm[@]}"
echo
echo -e "\033[1;31mGlideIn problem (?) sites:\033[0;30m\n ${listGlideIn[@]}"
echo
echo -e "\033[1;34mSites with special status:\033[0;30m\n ${listStatus[@]}"

rm ${TMPDIR}/condor_overview.txt
rm ${TMPDIR}/condor_overview.tmp
rm ${TMPDIR}/slot-limits.conf
