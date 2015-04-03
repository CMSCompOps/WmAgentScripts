from assignSession import *              

def prepateAssignment( ):
    ## pick up the list of everything
    # churn it
    # create
    parameters = {
        'SiteWhitelist' : None,
        'AcquisitionEra' : None,
        }
    from assignSession import *
    wf_params = {}
    ## those to be checked and closed
    for wfo in session.query(Workflow).filter(Workflow.status=='away').all():
        wf_params[wfo.name] = {}

    return wf_params


if not pending_transfer:
            #### if no transfer requests were made or none pending : ASSIGN        
            ## get the basic information of the campaigns involved
    campaigns = wfh.getCampaign()
    params = {}
    params_from_campaign = {}
# contains lfn, team
    
    version = 1
    outputs = []
            # check for existence and increase the version if possible (valid/production check)
    
    params['AcquisitionEra'] = wfh.acquisitionEra()
    params['SiteWhiteList'] = sites_allowed
    assignWorkflow( url, wf, team, params )

        

        
            
        
        
        
