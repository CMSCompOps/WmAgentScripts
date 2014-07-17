#!/bin/bash
source /data/admin/wmagent/env.sh
condor_rm -constraint 'JobStatus == 3 && ( CurrentTime - EnteredCurrentStatus ) > 3600' -forcex
condor_rm -constraint 'JobStatus == 5 && ( CurrentTime - EnteredCurrentStatus ) > 86400'
#condorq | awk '{if ($2==3) print $1}' | xargs condor_rm
#condorq | awk '{if ($2==5) print $1}' | xargs condor_rm
#condorq | grep T2_CH_CERN | grep LogCollect | awk '{print $1}' | xargs condor_rm

 
