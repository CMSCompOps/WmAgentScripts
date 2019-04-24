#log the memory of all
for each in 0268 0269 0272 0273 0274 0275; do
    lf='/data/unified/www/logs/checkmem/'`date +%F_%T`.$each.memlog
    date > $lf
    ssh vocms$each "ps -o pid,user,rss,%mem,args --sort -pmem -e | head -10" >> $lf
done
## keep it clean
find /data/unified/www/logs/checkmem/ -type f -name '*.memlog' -mtime +1 -exec rm {} \;
