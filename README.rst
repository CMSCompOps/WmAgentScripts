WMAgent Scripts
===============

.. contents:: :local:

Setting Up Scripts
------------------

Download the Scripts
~~~~~~~~~~~~~~~~~~~~

The easiest wy to download the WmAgentScripts is using git on lxplus or your own machine::

    git clone https://github.com/CMSCompOps/WmAgentScripts.git

.. Note::

  Does this work? I know a lot of things depend on dbsClient.
  OpsSpace tries to install this, but it's not tested yet.

Creating Proxy
~~~~~~~~~~~~~~

Most of the scripts need to load a proxy, so first you need to setup a certificate:
`New Operator Setup <https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsWorkflowNewOperatorSetup>`_

On SL6
~~~~~~

Generate your proxy::

    voms-proxy-init -voms cms

nType your key password and should display something like this::

    Contacting voms.cern.ch:15002 [/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch] "cms"... Remote VOMS server contacted succesfully. Created proxy in /tmp/x509up_uXXXX. Your proxy is valid until Thu Oct 09 21:53:28 CEST 2014

Export the X509_USER_PROXY variable to the environment (so it can be used by python), **use proxy location in the previous step**::

    export X509_USER_PROXY=/tmp/x509up_uXXXX

This is a one line command for all this procedure::

    export X509_USER_PROXY=$(voms-proxy-init -voms cms | grep Created | cut -c18- | tr -d '.')

On SL5
~~~~~~

Just kidding!
But the Twiki I'm copying this from looks pretty old, so this README needs updating.

Loading WMAgent Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some of the scripts need WMAgent libraries, which at the moment are only installed on WMAgent machines.
(i.e. vocms049 ... vocms174)
Log in to the machine and type::

    source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh

Scripts that interact with the Request Manager
----------------------------------------------

.. Note::

   I'll leave this up to you guys, but here's an example of how to display the help message.

reject.py
~~~~~~~~~

.. program-output:: python ../WmAgentScripts/reject.py -h

setCascadeStatus.py
~~~~~~~~~~~~~~~~~~~

.. program-output:: python ../WmAgentScripts/setCascadeStatus.py -h

reqMgrClient.py
~~~~~~~~~~~~~~~

.. automodule:: WmAgentScripts.Unified.reqMgrClient

.. program-output:: python ../WmAgentScripts/Unified/reqMgrClient.py -h

reqmgr.py
~~~~~~~~~

.. automodule:: WmAgentScripts.reqmgr

.. program-output:: python ../WmAgentScripts/reqmgr.py -h

Some examples:

- Create a request using the file julian.json::

    python WmAgentScripts/reqmgr.py -u https://cmsweb.cern.ch -i -f julian.json

- Assigning an existing request in ReqMgr (jbadillo_StoreResults_51816_v1_140826_100602_3071) changing splitting according to julian.json::

    python WmAgentScripts/reqmgr.py -u https://cmsweb.cern.ch -p -g -f julian.json -r jbadillo_StoreResults_51816_v1_140826_100602_3071

changeSplittingWorkflow.py
~~~~~~~~~~~~~~~~~~~~~~~~~~

This script allows to change the splitting of a request, on a given task name

.. program-output:: python ../WmAgentScripts/changeSplittingWorkflow.py -h

.. Note::
   - The TASKPATH should be the full task path in which you want to change the splitting, i.e. StepOneProc, StepOne /StepOneProcMerge, Production, etc.
   - The TYPE is the algorithm for splitting.

forceCompleteWorkflows.py
~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: WmAgentScripts.forceCompleteWorkflows

.. program-output:: python ../WmAgentScripts/forceCompleteWorkflows.py -h

getInputLocation.py
~~~~~~~~~~~~~~~~~~~

.. program-output:: python ../WmAgentScripts/getInputLocation.py -h

And So On
---------

I'm not sure how many of these scripts are actually being used, so I'll stop now.
