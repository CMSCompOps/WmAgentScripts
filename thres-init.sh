#!/bin/bash
MANAGEDIR="/data/srv/wmagent/current"
SITELIST="T3_US_PuertoRico T2_FI_HIP T2_UK_SGrid_RALPP T2_FR_GRIF_LLR T3_US_Baylor T3_UK_London_QMUL T3_TW_NTU_HEP T3_US_Omaha T2_KR_KNU T2_RU_SINP T3_US_UMD T1_TW_ASGC T3_US_Colorado T1_UK_RAL_Disk T3_IT_Napoli T3_NZ_UOA T3_GR_IASA T2_IT_Bari T2_US_UCSD T2_RU_IHEP T3_US_Vanderbilt_EC2 T2_RU_RRC_KI T3_US_JHU T3_BY_NCPHEP T1_US_FNAL_Disk T3_US_UTENN T3_US_UCR T3_TW_NCU T2_CH_CSCS T2_UA_KIPT T2_PK_NCP T2_RU_PNPI T3_UK_ScotGrid_ECDF T3_UK_London_UCL T3_US_Brown T3_US_UCD T3_CO_Uniandes T2_FR_IPHC T3_US_OSU T3_US_TAMU T1_US_FNAL T2_IT_Rome T2_UK_London_Brunel T2_PL_Cracow T3_IT_Trieste T2_EE_Estonia T2_IN_TIFR T2_CN_Beijing T2_US_Florida T3_US_Princeton_ICSE T3_IT_MIB T3_US_FNALXEN T3_US_Rutgers T1_DE_KIT T2_US_Wisconsin T2_HU_Budapest T2_DE_RWTH T2_US_Vanderbilt T2_BR_SPRACE T2_PT_LIP_Lisbon T2_CH_CERN T2_BR_UERJ T3_MX_Cinvestav T3_US_FNALLPC T1_UK_RAL T3_IT_Firenze T3_US_Cornell T2_ES_IFCA T3_US_UVA T3_ES_Oviedo T3_US_NotreDame T2_DE_DESY T3_US_UIowa T2_US_Caltech T3_FR_IPNL T2_TW_Taiwan T3_UK_London_RHUL T0_CH_CERN T3_CN_PKU T2_UK_London_IC T2_US_Nebraska T2_ES_CIEMAT T3_UK_ScotGrid_GLA T3_DE_Karlsruhe T3_US_FSU T3_KR_UOS T3_IT_Perugia T1_IT_CNAF T2_TR_METU T2_AT_Vienna T2_US_Purdue T3_US_Rice T2_BE_UCL T3_US_FIT T2_UK_SGrid_Bristol T2_PT_NCG_Lisbon T1_ES_PIC T2_IT_Legnaro T2_RU_ITEP T2_RU_JINR T2_IT_Pisa T2_GR_Ioannina T3_UK_SGrid_Oxford T1_FR_CCIN2P3 T2_FR_GRIF_IRFU T3_US_UMiss T3_US_UCLA T2_FR_CCIN2P3 T2_PL_Warsaw T3_US_TTU T2_US_MIT T2_BE_IIHE T2_RU_INR T3_CH_PSI T3_IT_Bologna"

usage()
{
	echo "$0"
}

init_thres()
{
	SITE=$1
	echo "Site: $i"
	$MANAGEDIR/config/wmagent/manage execute-agent wmagent-resource-control --site-name=$SITE --task-type=Merge --task-slots=100
	$MANAGEDIR/config/wmagent/manage execute-agent wmagent-resource-control --site-name=$SITE --task-type=LogCollect --task-slots=100
	$MANAGEDIR/config/wmagent/manage execute-agent wmagent-resource-control --site-name=$SITE --task-type=Cleanup --task-slots=100
}

echo -n "Retrieving site list..."
#LIST=$(./config/wmagent/manage execute-agent wmagent-resource-control -p | awk '/^T/{print $1}')
LIST=$SITELIST
echo $LIST
for i in $LIST; do
	init_thres $i
done
