#!/usr/bin/env python

from condor_overview import *
try:
    import htcondor
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)


global_pool = 'vocms099.cern.ch' #"vocms97.cern.ch"

# production schedd's
schedds = ["vocms0304.cern.ch",
           "vocms0308.cern.ch",
           "vocms0309.cern.ch",
           "vocms0310.cern.ch",
           "vocms0311cern.ch",
           "cmssrv217.fnal.gov",
           "cmssrv218.fnal.gov",
           "cmssrv219.fnal.gov",
           "cmsgwms-submit1.fnal.gov",
           "cmsgwms-submit2.fnal.gov",
           ]

def main():
    overview_running = {}
    overview_pending = {}
    overview_other = {}
    overview_running48 = {}
    overview_numjobstart = {}
    overview_removereason = {}
    jobs_48 = {}
    jobs_maxwall = {}
    jobs_numjobstart = {}
    jobs_removereason = {}
    
    # global pool collector
    coll = htcondor.Collector(global_pool)
    schedd_ads = coll.query(htcondor.AdTypes.Schedd, 'CMSGWMS_Type=?="prodschedd"', ['Name', 'MyAddress', 'ScheddIpAddr'])
    
    # all schedds 
    for ad in schedd_ads:
        if ad["Name"] not in schedds:
            continue
        print "getting jobs from %s"%ad["Name"]
        #fill the overview
        get_overview(overview_running,
                        overview_pending,
                        overview_other,
                        overview_running48,
                        overview_numjobstart,
                        overview_removereason,
                        jobs_48,
                        jobs_maxwall,
                        jobs_numjobstart,
                        jobs_removereason,
                        ad)
        
    print_results(overview_running,
                    overview_pending,
                    overview_running48,
                    overview_numjobstart,
                    overview_removereason,
                    jobs_48,
                    jobs_maxwall,
                    jobs_numjobstart,
                    jobs_removereason)

if __name__ == "__main__":
    main()
    