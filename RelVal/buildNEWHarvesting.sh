#!/bin/bash
if [ $# -eq 1 ]; then
  path=$1
else
  echo "Usage: source /data/amaltaro/buildHarvesting.sh <path_to_stats_file>"
  echo "Example: source /data/amaltaro/buildHarvesting.sh ~/webpage/relval_stats/CMSSW_5_3_2_standard_v1.txt"
  return
fi

echo "#!/bin/bash" > DQM.sh

echo "### ********** MC FullSim DQM --> --scenario=relvalmc **************" >> DQM.sh
for dset in `cat $path | grep '/DQM' | grep -v '\-1' | grep '/RelVal' | grep -v FastSim | egrep -v "RelValHydjetQ|RelValPyquen" | awk '{print $1}' | sed 's/|//g'`; 
do 
#python harvestDQMDataset.py --dataset=/MinimumBias/Run2012D-08Oct2012_PRReference_R203835-v1/DQM --scenario=pp --dqm-url=https://cmsweb.cern.ch/dqm/relval --dqm-sequences=@commonSiStripZeroBias,@ecal,@hcal,@muon --job-dir=./Reference
  out="python harvestDQMDataset.py --dataset=$dset --dqm-url=https://cmsweb.cern.ch/dqm/relval --scenario=relvalmc"
  jobDir=`echo $dset | awk -F'/' '{print $2"____"$3}'`
  line=`echo "$out --job-dir=$jobDir"` 
  echo $line >> DQM.sh
done
echo "### ************ MC FullSim HeavyIon --> --scenario=HeavyIons ************" >> DQM.sh
for dset in `cat $path | egrep "RelValHydjetQ|RelValPyquen" | grep -v '\-1' | grep '/DQM' | grep '/RelVal' | awk '{print $1}' | sed 's/|//g' `;
do
  out="python harvestDQMDataset.py --dataset=$dset --dqm-url=https://cmsweb.cern.ch/dqm/relval --scenario=HeavyIons"
#  out=`/data/amaltaro/WmAgentScripts/dbssql --input="find dataset where dataset.status=* and dataset=$dset" | grep RelVal | awk '{printf("python harvestDQMDataset.py --dataset=%s --dqm-url=https://cmsweb.cern.ch/dqm/relval --scenario=HeavyIons",$1)}'`
  jobDir=`echo $dset | awk -F'/' '{print $2"____"$3}'`
  line=`echo "$out --job-dir=$jobDir"`
  echo $line >> DQM.sh
done
echo "### ************ FastSim GEN-SIM-RECO --> --scenario=relvalmcfs ************" >> DQM.sh
for dset in `cat $path | grep FastSim | grep -v '/DQM' | awk '{print $1}' | sed 's/|//g' `; 
#for dset in `cat $path | grep FastSim | grep -v '\-1' | grep -v '/DQM' | awk '{print $1}' | sed 's/|//g' `; 
do     
  out="python harvestDQMDataset.py --dataset=$dset --dqm-url=https://cmsweb.cern.ch/dqm/relval --scenario=relvalmcfs"
  jobDir=`echo $dset | awk -F'/' '{print $2"____"$3}'`
  line=`echo "$out --job-dir=$jobDir"`
  echo $line >> DQM.sh
done

echo "### ************ Real DQM data PP --> --scenario=pp ************" >> DQM.sh
for dset in `cat $path | grep '/DQM' | grep -v '\-1' | grep '_RelVal_' | egrep -v 'Cosmics|HIAll' | awk '{print $1}' | sed 's/|//g' `;
do
  jobDir=`echo $dset | awk -F'/' '{print $2"____"$3}'`
  out="python harvestDQMDataset.py --dataset="$dset" --dqm-url=https://cmsweb.cern.ch/dqm/relval --scenario=pp"
  line=$out" --job-dir="$jobDir
  echo $line >> DQM.sh
done

echo "### ************ Real DQM data Cosmics --> --scenario=cosmics ************" >> DQM.sh
for dset in `cat $path | grep Cosmics | grep -v '\-1' | grep '/DQM' | grep '_RelVal_' | awk '{print $1}' | sed 's/|//g' `;
do
  jobDir=`echo $dset | awk -F'/' '{print $2"____"$3}'`
  out=`/data/amaltaro/WmAgentScripts/dbssql --input="find dataset, run where dataset.status=* and dataset=$dset" | grep RelVal | awk '{printf("python harvestDQMDataset.py --dataset=%s --dqm-url=https://cmsweb.cern.ch/dqm/relval --run-number=%s --scenario=cosmics",$1,$2)}'`
  line=`echo "$out --job-dir=$jobDir"`
  echo $line >> DQM.sh
done

echo "### ************ Real DQM data HeavyIon --> --scenario=HeavyIons ************" >> DQM.sh
for dset in `cat $path | egrep "HIAll" | grep -v '\-1' | grep '/DQM' | grep '_RelVal_' | awk '{print $1}' | sed 's/|//g' `;
do
  jobDir=`echo $dset | awk -F'/' '{print $2"____"$3}'`
  out=`/data/amaltaro/WmAgentScripts/dbssql --input="find dataset, run where dataset.status=* and dataset=$dset" | grep RelVal | awk '{printf("python harvestDQMDataset.py --dataset=%s --dqm-url=https://cmsweb.cern.ch/dqm/relval --run-number=%s --scenario=HeavyIons",$1,$2)}'`
  line=`echo "$out --job-dir=$jobDir"`
  echo $line >> DQM.sh
done
echo "### ********** MC Generator --> --scenario=relvalgen **************" >> DQM.sh
for dset in `cat $path | grep '/RelVal' | grep -v FastSim | awk '{print $1}' | sed 's/|//g' | grep '/GEN$'`;
do
  out="python harvestDQMDataset.py --dataset=$dset --dqm-url=https://cmsweb.cern.ch/dqm/relval --scenario=relvalgen"
  jobDir=`echo $dset | awk -F'/' '{print $2"____"$3}'`
  line=`echo "$out --job-dir=$jobDir"`
  echo $line >> DQM.sh
done
