#!/bin/bash

dsets_tmpfile=`mktemp`
releases_tmpfile=`mktemp`

echo \$dsets_tmpfile
echo $dsets_tmpfile

echo \$releases_tmpfile
echo $releases_tmpfile

echo "getting list of datasets at T2_CH_CERN"

python2.6 /data/relval/WmAgentScripts/RelVal/das_client.py --query="dataset dataset=/*RelVal*/*/* site = T2_CH_CERN" --limit 0 >& $dsets_tmpfile
python2.6 /data/relval/WmAgentScripts/RelVal/das_client.py --query="dataset dataset=/*/*RelVal*/* site = T2_CH_CERN" --limit 0 >> $dsets_tmpfile

echo "finished getting the list of datasets at T2_CH_CERN"

echo "getting the sizes of the datasets"

python2.6 /data/relval/WmAgentScripts/RelVal/get_dset_sizes.py $dsets_tmpfile >& ~/webpage/eos_space/datasets.txt

echo "finished getting the sizes of the datasets"

cat ~/webpage/eos_space/datasets.txt | sed "s/\// /" | sed "s/\// /" | sed "s/\// /" | awk '{print $2}' | sed "s/-/ /" | awk '{print $1}' | sort | uniq >& $releases_tmpfile

for rel in `cat $releases_tmpfile`; do echo -n "release: $rel"; grep ${rel}- ~/webpage/eos_space/datasets.txt | awk '{SUM+=$2} END {printf " size: "SUM"\n"}'; done >& ~/webpage/eos_space/releases_sizes_bytes.txt

cat ~/webpage/eos_space/releases_sizes_bytes.txt | awk '{printf "release: %35s     size: %6.2f TB\n",$2,$4/1000000000000}'>& ~/webpage/eos_space/releases_sizes.txt
cat ~/webpage/eos_space/releases_sizes_bytes.txt | awk '{SUM+=$4} END {printf ("Total  : %35s     size: %6.2f TB\n","",SUM/1000000000000)}' >> ~/webpage/eos_space/releases_sizes.txt 

#cat ~/webpage/datasets_EOS/releasesSpaceAtEOS.BKP | awk '{if ($4 >0.1) print ;}' > ~/webpage/datasets_EOS/releasesSpaceAtEOS
