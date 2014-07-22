#!/bin/bash
./config/wmagent/manage execute-agent wmcoreD --shutdown --components=PhEDExInjector
./config/wmagent/manage mysql-prompt wmagent<querry.sql>lfns.txt
cat lfns.txt | grep /store > lfnsgreped.txt
#tail -n 28 install/wmagent/PhEDExInjector/ComponentLog | head -n1 > phedex.log
#for j in $(for i in $(cat phedex.log) ; do echo $i | awk -F'"' {'print $2'} ; done | grep root) ; do echo $j; done > lfns.txt
./fileInPhedex.py lfnsgreped.txt > lfnsinPhedex.txt
for i in $(cat lfnsinPhedex.txt); do echo "UPDATE dbsbuffer_file set in_phedex = 1 where lfn='$i';";done > meuscript.sql
./config/wmagent/manage mysql-prompt wmagent < meuscript.sql
./config/wmagent/manage execute-agent wmcoreD --shutdown --components=PhEDExInjector
./config/wmagent/manage execute-agent wmcoreD --start --components=PhEDExInjector
