#!/bin/bash

wf_names_file=$1
statistics_file_name=$2
dsets_tmp_cern=`mktemp`
dsets_tmp_fnal=`mktemp`
dsets_stats_tmp=`mktemp`

get_output_datasets_script=`mktemp`
phedex_subscriptions_script=`mktemp`
dset_status_script=`mktemp`
wf_status_script=`mktemp`

echo "------> closing out workflows"
echo "" 
source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh
source /afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid-env.sh 
python2.6 closeOutTaskChainWorkflows.py $wf_names_file
echo ""
echo "------> finished closing out workflows"

echo ""
echo "------> press enter to continue"
read
echo ""

echo "------> getting dataset names"
echo ""
python2.6 getRelValDsetNames.py $wf_names_file | tee $dsets_stats_tmp
echo ""
echo "------> finished getting dataset names"

echo "------> making statistics table"
echo ""
python2.6 makeStatisticsTable.py $dsets_stats_tmp ~/webpage/relval_stats/$statistics_file_name
echo ""
echo "------> finished making statistics table"

echo "------> making phedex subscriptions"
echo ""
awk '{print $1}' $dsets_stats_tmp >& $dsets_tmp_fnal
grep -v '/RECO\>' $dsets_tmp_fnal >& $dsets_tmp_cern
echo \$dsets_tmp_fnal
echo $dsets_tmp_fnal
echo \$dsets_tmp_cern
echo $dsets_tmp_cern
python2.6 phedexSubscription.py T2_CH_CERN $dsets_tmp_cern \"relval datasets\"
python2.6 phedexSubscription.py T1_US_FNAL_MSS $dsets_tmp_fnal \"relval datasets\" --custodial
echo ""
echo "------> finished making phedex subscriptions"

echo "------> setting datasets to valid"
echo ""
for dset in `cat $dsets_tmp_fnal`; do python2.6 setDatasetStatusDBS_2_3.py --dataset=$dset --status=VALID --files; done
echo ""
echo "------> finished setting datasets to valid"

echo "------> writing announcement e-mail"
echo ""
echo "Dear all,"
echo ""
echo "The following datasets are now available:"
echo "http://cms-project-relval.web.cern.ch/cms-project-relval/relval_stats/$statistics_file_name"
echo ""
echo "HN request:"
echo ""
echo "Best regards,"
echo "Andrew and Alan" 
echo ""
echo "------> finished writing announcement e-mail"

echo ""
echo "------> press enter to continue"
#read
echo ""

echo "------> setting workflows to announced"
echo ""
#python2.6 setrequeststatus.py $wf_names_file announced
echo ""
echo "------> finished setting workflows to announced"
