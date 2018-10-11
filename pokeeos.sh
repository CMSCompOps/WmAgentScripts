#!/bin/bash

out=/data/unified-cache/listingeos.$$.$HOSTNAME.log
date > $out
ls /eos/cms/store/unified/ >> $out
date >> $out
echo >> $out
ls /eos/cms/store/unified/www/ >>$out
date >> $out
echo >> $out
ls /eos/cms/store/logs/prod/2018/* -l >>$out
date >> $out

cp $out /eos/cms/store/unified/www/eospoke/.
ls /eos/cms/store/unified/www/eospoke/*.log
