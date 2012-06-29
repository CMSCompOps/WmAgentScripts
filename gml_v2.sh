#!/bin/sh
#
# 2012-06-21
# Global Monitor Light script

TMPDIR=/tmp/cmst1
BASEDIR=~cmst1/scripts
treswarn=95
tresalarm=70

BLU='\033[0;34m'
GRE='\033[0;32m'
DEF='\033[0;30m'
RED='\033[0;31m'
CYA='\033[0;36m'
PUR='\033[0;35m'
backg="\033[48m"


# USAGE #
#########
Manual=`echo -e "${BLU}Options:${DEF}"'
	'"${RED}-help / -h${DEF}   : print these options."'
	'"${RED}-lines / -l${DEF}  : add extra horizontal lines for readability."'
        '"${RED}-backgr / -b${DEF} : alternate background color for readability."'
'`

Usage()
{
  echo "$Manual" ; exit ;
}

# OPTIONS #
###########
hlines=0
for arg in $*; do
	case $arg in
		-lines)		hlines=1;	shift;;
		-l)		hlines=1;	shift;;
		-backgr)	hlines=2;	shift;;
		-b)		hlines=2;	shift;;
		-help)		Usage;;
		-h)		Usage;;
	esac
done

# MAIN #
########
echo -e "\033[0;34mGetting slot limits (slot-limits.conf)...\033[m"
if [ -f ${TMPDIR}/slot-limits.conf ]; then rm ${TMPDIR}/slot-limits.conf; fi
wget -P ${TMPDIR} https://cmst1.web.cern.ch/CMST1/wmaconfig/slot-limits.conf 2> /dev/null 
echo "  Done."

echo -e "\033[0;34mGetting global condor_overview...\033[m"
if [ -f ${TMPDIR}/condor_overview.txt ]; then rm ${TMPDIR}/condor_overview.txt; fi
`python /afs/cern.ch/user/c/cmst1/condor_global_overview.py > ${TMPDIR}/condor_overview.txt`
echo "  Done."
#cat condor_overview_all > ${TMPDIR}/condor_overview.txt

echo -e "\033[0;34mGetting site info from cms dashboard...\033[m"
if [ -f ${TMPDIR}/site_view.summary ]; then rm ${TMPDIR}/site_view.summary; fi
`python /afs/cern.ch/user/c/cmst1/scripts/site_view.py 2> /dev/null`
echo "  Done."
#`python ${BASEDIR}/site_view.py`
echo

T2List=`(cat ${BASEDIR}/T2List.txt | awk -F"|" '{print $2}' | sed "s: ::g")`
slotList=`(cat ${TMPDIR}/slot-limits.conf | awk '{print $1}')`
siteList=`(cat ${TMPDIR}/site_view.summary | awk '{print $1}')`

printf " %-20s | %-10s | %-10s | %-10s | %-10s | %-10s | %-20s | %-30s\n"   T2 Pending Running Slots Threshold Status 'Readiness (CE/SRM)' Tickets
echo   "----------------------+------------+------------+------------+------------+------------+----------------------+--------------------------------+" 

check=false
nlines=0

for T2 in $T2List ; do
  echo -ne ${backg}
  if [ ! $check == true -a "${T2:0:2}" == "T2" ]; then echo "----------------------+------------+------------+------------+------------+------------+----------------------+--------------------------------+"; check=true; fi

  slots=`(cat ${BASEDIR}/T2List.txt | grep $T2 | awk -F"|" '{print $4}' | sed "s: ::g")`
  if [ `echo $slots | grep ",5"` ]; then slots=${slots:0:$((${#slots}-2))}; fi
  
  ready1=`(cat ${TMPDIR}/site_view.summary) | grep $T2 | awk -F"|" '{print $4}' | sed "s: ::g"`
  ready2=`(cat ${TMPDIR}/site_view.summary) | grep $T2 | awk -F"|" '{print $5}' | sed "s: ::g"`

  ticketsLong=`(cat ${TMPDIR}/site_view.summary) | grep $T2 | awk -F"|" '{print $17}' | sed "s: ::g"`
  ticketsEdit=`echo $ticketsLong | awk -F"?" '{print $1}'``echo $ticketsLong | awk -F"?" '{print $2}' | awk -F"&" '{print "?"$2"&"$7}'`
   if [ -n $ticketsLong ] && [ ! "$ticketsLong" == None ]; then
    tickets=${backg}`curl http://tinyurl.com/api-create.php?url=${ticketsEdit} 2> /dev/null`"\033[48m";
  else
  	tickets="${backg}\033[48m";
	oldBackg=${backg};
  fi

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
  
  threshold=`(grep $T2 ${TMPDIR}/slot-limits.conf | awk '{print $2}')`
  status=`(grep $T2 ${TMPDIR}/slot-limits.conf | awk '{print $3}')`

  if [ $(($nlines%2)) == 0 ] && [ ! $nlines == 0 ] && [ $hlines == 1 ]; then
      echo "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ";
  fi
  if [ $(($nlines%2)) == 1 ] && [ ! $nlines == 0 ] && [ $hlines == 2 ]; then
      backg="\033[47m"
  fi

  echo -ne ${backg} 

  # colorizing
  oPend=$Pend
  if [ "$oPend" == "0" ]; then Pend="\033[0;31m${backg}$oPend\033[0;30m${backg}"; fi

  oRun=$Run
  oT2=$T2
  
  if ([ "$ready1" == "CRITICAL" ] || [ "$ready2" == "CRITICAL" ]); then
  	ready="\033[0;31m${backg}$ready1/$ready2\033[0;30m${backg}";
  	lread=$((${#ready}-36));
  elif ([ "$ready1" == "WARNING" ] || [ "$ready2" == "WARNING" ]) && ([ ! "$ready1" == "CRITICAL" ] || [ ! "$ready2" == "CRITICAL" ]); then
   	ready="\033[0;33m${backg}$ready1/$ready2\033[0;30m${backg}";
  	lread=$((${#ready}-36));
  elif ([ "$ready1" == "OK" ] || [ "$ready2" == "OK" ]) && ([ ! "$ready1" == "WARNING" ] || [ ! "$ready2" == "WARNING" ] || [ ! "$ready1" == "CRITICAL" ] || [ ! "$ready2" == "CRITICAL" ]) ; then
  	ready="\033[0;35m${backg}\033[0;30m${backg}";
  	lread=$((${#ready}-36));
  else
    	ready="\033[0;30m${backg}$ready1/$ready2\033[0;30m${backg}";
  	lread=$((${#ready}-36));
  fi
  
  if [ "$oRun" -lt $(($slots*$tresalarm/100)) -a "$oRun" -gt 0 -a "$oPend" -gt 0 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then Run="\033[1;31m${backg}$oRun\033[0;30m${backg}"; T2="\033[0;31m${backg}$oT2\033[0;30m${backg}"; if [ "$lread" == 0 ]; then if [ "$tickets" == "${oldBackg}\033[48m" ]; then listAlarmNoSiteWarn=(${listAlarmNoSiteWarn[@]}" $oT2 is running $oRun, $slots slots.\n");  else listAlarmNoSiteWarn=(${listAlarmNoSiteWarn[@]}" $oT2 is running $oRun, $slots slots ($tickets).\n"); fi; else if [ "$tickets" == "${oldBackg}\033[48m" ]; then listAlarmSiteWarn=(${listAlarmSiteWarn[@]}" $oT2 is running $oRun, $slots slots.\n"); else listAlarmSiteWarn=(${listAlarmSiteWarn[@]}" $oT2 is running $oRun, $slots slots ($tickets).\n"); fi; fi; fi
  if [ "$oRun" -lt $(($slots*$treswarn/100)) -a "$oRun" -gt $(($slots*tresalarm/100)) -a "$oPend" -gt 0 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then Run="\033[1;35m${backg}$oRun\033[0;30m${backg}"; T2="\033[0;35m${backg}$oT2\033[0;30m${backg}"; if [ "$lread" == 0 ]; then if [ "$tickets" == "${oldBackg}\033[48m" ]; then listWarnNoSiteWarn=(${listWarnNoSiteWarn[@]}" $oT2 is running $oRun, $slots slots.\n"); else listWarnNoSiteWarn=(${listWarnNoSiteWarn[@]}" $oT2 is running $oRun, $slots slots ($tickets).\n"); fi; else if [ "$tickets" == "${oldBackg}\033[48m" ]; then listWarnSiteWarn=(${listWarnSiteWarn[@]}" $oT2 is running $oRun, $slots slots.\n"); else listWarnSiteWarn=(${listWarnSiteWarn[@]}" $oT2 is running $oRun, $slots slots ($tickets).\n"); fi; fi; fi
  if [ "$oRun" == "0" ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then Run="\033[0;31m${backg}$oRun\033[0;30m${backg}"; T2="\033[0;31m${backg}$oT2\033[0;30m${backg}"; fi
  if [ "$oRun" == "0" ]; then Run="\033[0;31m${backg}$oRun\033[0;30m${backg}"; fi

  # making lists
  if [ "$oPend" -lt 10 ] && [ "$oRun" -lt 10 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then listEmpty=(${listEmpty[@]}" $oT2 is running $oRun, has $oPend pending, $slots slots --> empty\n"); fi
  if [ "$oRun" -lt "10" ]  && [ "$oPend" -gt 10 ] && [ ! "$status" == "drain" ] && [ ! "$status" == "skip" ] && [ ! "$status" == "down" ]; then listGlideIn=(${listGlideIn[@]}" $oT2 is running $oRun, has $oPend pending, $slots slots --> GlideIn problem?\n"); fi
  if [ "$status" == "drain" ] ; then listDrain=(${listDrain[@]}" $oT2 status $status: still $oRun running, $oPend pending.\n"); fi
  if [ "$status" == "down" ]; then T2="${backg}\033[0;37m$oT2\033[0;30m${backg}"; listDown=(${listDown[@]}" $oT2 status $status: still $oRun running, $oPend pending.\n"); fi

#  if [ "$status" == "drain" ]; then T2="\033[0;30m\033[0;43m$oT2\033[0m\033[0m"; fi
#  if [ "$status" == "down" ]; then T2="\033[0;30m\033[0;41m$oT2\033[0m\033[0m"; fi

  printf " %b%*s | %b%*s | %b%*s | %-10s | %-10s | %-10s |"   $T2 $((20-${#oT2})) "" $Pend $((10-${#oPend})) "" $Run $((10-${#oRun})) "" $slots $threshold $status
  printf " %b%*s |" $ready $((20-$lread)) "" 
  printf " %b%*s\033[m\n" $tickets $((30-${#tickets}+16)) ""

  nlines=$(($nlines+1)) 
  backg="\033[48m"
  echo -ne ${backg}
done

# repeat for sites missing in priority list
nlines=0
echo
echo -e "\033[4;34mSites not in the priority list (no auto-info):\033[0;m"
printf " %-20s | %-10s | %-10s | %-10s | %-10s | %-10s | %-20s | %-30s\n"   T2 Pending Running Slots Threshold Status 'Readiness (CE/SRM)' Tickets
echo   "----------------------+------------+------------+------------+------------+------------+----------------------+--------------------------------+" 

for SL in $slotList; do
	flagT2=0;
	flagSI=0;
	for T2 in $T2List; do
		if [[ $SL == *$T2* ]]; then 
			flagT2=1;
			break;
		fi
	done
	for SI in $siteList; do
		if [[ $SL == *$SI* ]]; then
			flagSI=1;
			break;
		fi
	done
	if [ $flagT2 == 0 ]; then
		echo -ne ${backg}
		
		slots="N/A"		
		if [ $flagSI == 1 ]; then
			ready1=`(cat ${TMPDIR}/site_view.summary) | grep $SL | awk -F"|" '{print $4}' | sed "s: ::g"`
			ready2=`(cat ${TMPDIR}/site_view.summary) | grep $SL | awk -F"|" '{print $5}' | sed "s: ::g"`
			
			ticketsLong=`(cat ${TMPDIR}/site_view.summary) | grep $SL | awk -F"|" '{print $17}' | sed "s: ::g"`
			ticketsEdit=`echo $ticketsLong | awk -F"?" '{print $1}'``echo $ticketsLong | awk -F"?" '{print $2}' | awk -F"&" '{print "?"$2"&"$7}'`
			if [ -n $ticketsLong ] && [ ! "$ticketsLong" == None ]; then
				tickets=${backg}`curl http://tinyurl.com/api-create.php?url=${ticketsEdit} 2> /dev/null`"\033[48m";
			else
				tickets="${backg}\033[48m";
				oldBackg=${backg};
			fi
		fi
		Run=0
		cat ${TMPDIR}/condor_overview.txt | sed -n '/Running/,/Total/p' > ${TMPDIR}/condor_overview.tmp
		if (grep -q "$SL" ${TMPDIR}/condor_overview.tmp); then
			AddRun=`(cat ${TMPDIR}/condor_overview.txt |  sed -n '/Running/,/Total/p' | grep $SL | awk -F"|" '{print $8}' | sed "s: ::g")`
			for Add in $AddRun ; do
				Run=`expr $Run + $Add`
			done
		fi
		Pend=0
		cat ${TMPDIR}/condor_overview.txt | sed -n '/Pending/,/Total/p' > ${TMPDIR}/condor_overview.tmp
		if (grep -q "$SL" ${TMPDIR}/condor_overview.tmp); then
			AddPend=`(cat ${TMPDIR}/condor_overview.txt |  sed -n '/Pending/,/Total/p' | grep $SL | awk -F"|" '{print $8}' | sed "s: ::g")`
			for Add in $AddPend ; do
				Pend=`expr $Pend + $Add`
			done
		fi

		threshold=`(grep $SL ${TMPDIR}/slot-limits.conf | awk '{print $2}')`
		status=`(grep $SL ${TMPDIR}/slot-limits.conf | awk '{print $3}')`

		if [ $(($nlines%2)) == 0 ] && [ ! $nlines == 0 ] && [ $hlines == 1 ]; then
			echo "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ";
		fi
		if [ $(($nlines%2)) == 1 ] && [ ! $nlines == 0 ] && [ $hlines == 2 ]; then
			backg="\033[47m"
		fi
		
		echo -ne ${backg} 

		# colorizing
		oPend=$Pend
		if [ "$oPend" == "0" ]; then Pend="\033[0;31m${backg}$oPend\033[0;30m${backg}"; fi

		oRun=$Run
		oSL=$SL

		if ([ "$ready1" == "CRITICAL" ] || [ "$ready2" == "CRITICAL" ]); then
			ready="\033[0;31m${backg}$ready1/$ready2\033[0;30m${backg}";
			lread=$((${#ready}-36));
		elif ([ "$ready1" == "WARNING" ] || [ "$ready2" == "WARNING" ]) && ([ ! "$ready1" == "CRITICAL" ] || [ ! "$ready2" == "CRITICAL" ]); then
			ready="\033[0;33m${backg}$ready1/$ready2\033[0;30m${backg}";
			lread=$((${#ready}-36));
		elif ([ "$ready1" == "OK" ] || [ "$ready2" == "OK" ]) && ([ ! "$ready1" == "WARNING" ] || [ ! "$ready2" == "WARNING" ] || [ ! "$ready1" == "CRITICAL" ] || [ ! "$ready2" == "CRITICAL" ]) ; then
			ready="\033[0;35m${backg}\033[0;30m${backg}";
			lread=$((${#ready}-36));
		else
			ready="\033[0;30m${backg}$ready1/$ready2\033[0;30m${backg}";
			lread=$((${#ready}-36));
		fi
  
		printf " %b%*s | %b%*s | %b%*s | %-10s | %-10s | %-10s |"   $SL $((20-${#oSL})) "" $Pend $((10-${#oPend})) "" $Run $((10-${#oRun})) "" $slots $threshold $status
		printf " %b%*s |" $ready $((20-$lread)) "" 
		printf " %b%*s\033[m\n" $tickets $((30-${#tickets}+16)) ""

		nlines=$(($nlines+1)) 
		backg="\033[48m"
		echo -ne ${backg}

	fi
done


echo
echo
echo -e "\033[1;30m--GlideIn-TICKETS---------------------------------------------------------------\033[0;m"
glidein="https://savannah.cern.ch/support/index.php?group=cmscompinfrasup&assigned_to=7781"
gltickets=`curl http://tinyurl.com/api-create.php?url=${glidein} 2> /dev/null`
echo $gltickets
echo
echo
echo -e "\033[1;30m--SUMMARY-----------------------------------------------------------------------\033[0;m"
echo
echo -e "\033[4;31mEmpty sites:\033[0;30m\n ${listEmpty[@]}"
echo
echo "--------------------------------------------------------------------------------"
echo -e "\033[4;32mNo site issues reported on dashboard:\033[0;30m\n"
echo -e "\033[1;35mWarning sites ($tresalarm% < xx < $treswarn%):\033[0;30m\n ${listWarnNoSiteWarn[@]}"
echo -e "\033[1;31mAlarm sites (xx < $tresalarm%):\033[0;30m\n ${listAlarmNoSiteWarn[@]}"
echo
echo "--------------------------------------------------------------------------------"
echo -e "\033[4;31mExisting site issues on dashboard:\033[0;30m\n"
echo -e "\033[1;35mWarning sites ($tresalarm% < xx < $treswarn%):\033[0;30m\n ${listWarnSiteWarn[@]}"
echo -e "\033[1;31mAlarm sites (xx < $tresalarm%):\033[0;30m\n ${listAlarmSiteWarn[@]}"
echo
echo "--------------------------------------------------------------------------------"
echo -e "\033[4;31mGlideIn problem (?) sites:\033[0;30m\n ${listGlideIn[@]}"
echo
echo "--------------------------------------------------------------------------------"
echo -e "\033[4;34mSites with status \"drain\":\033[0;30m\n ${listDrain[@]}"
echo -e "\033[4;34mSites with status \"down\":\033[0;30m\n ${listDown[@]}"
echo


rm ${TMPDIR}/condor_overview.txt
rm ${TMPDIR}/condor_overview.tmp
rm ${TMPDIR}/slot-limits.conf
rm ${TMPDIR}/site_view.summary
