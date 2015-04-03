import sys
from injector import injector
from transferor import transferor


if __name__ == "__main__":
    url = 'cmsweb.cern.ch'
    wf = None
    phe = None
    if len(sys.argv)>1:
        if sys.argv[1].isdigit():
            phe = int(sys.argv[1])
        else:
            wf = sys.argv[1]

    ## fetch new workflows into db. replace failed
    injector(url) ## in acr

    ## create required data transfer
    # can only be set croned once we agree on spreading to all in white list
    transferor(url,wf) 
    
    ## complete the transfers
    stagor(url,phe) ## in acr

    ## assign the workflows that can
    # can only be set in cron once we are convinced it does not screw things up
    assignor(url,wf)


    ## close the workflows that can 
    closor(url,wf) ## in acr

    ## clean after us . not yet implemented    
    cleanor(url)
    
