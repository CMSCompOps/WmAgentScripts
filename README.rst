WMAgent Scripts
===============

Creating Proxy
~~~~~~~~~~~~~~
Most of the scripts need to load a proxy, so first you need to setup a certificate:
`New Operator Setup <https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsWorkflowNewOperatorSetup>`


Running the Unit Tests
~~~~~~~~~~~~~~
We need to do some workarounds to run the unit tests due to some build issues. Because of that only users with sudo access can run all of the unit tests at the moment.

After connecting at lxplus, follow the steps below::

    > ssh <your-user>@vocms0XXX.cern.ch
    > voms-proxy-init -voms cms
    > export X509_USER_PROXY=/tmp/x509up_uXXXXXX
    > exit
    > ssh root@vocms0XXX.cern.ch
    > source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
    > python3 setup.py develop
    > export CRYPTOGRAPHY_ALLOW_OPENSSL_102=1
    > export RUCIO_ACCOUNT="<your-user>"
    > export X509_USER_PROXY=/tmp/x509up_uXXXXXX``

To run only one test file::

    > python3 test/python/<path_to_file>

To run all of the test files::

    > coverage run --source=./src/python/  -m unittest discover ./test/python/ -p "*_t.py"
    
    
Deployment
~~~~~~~~~~~~~~

*Secrets:*

1. OracleDB secrets are located under `src/python/Utilities/`


