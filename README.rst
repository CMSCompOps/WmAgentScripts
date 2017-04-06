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

Type your key password and should display something like this::

    Contacting voms.cern.ch:15002 [/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch] "cms"... Remote VOMS server contacted succesfully. Created proxy in /tmp/x509up_uXXXX. Your proxy is valid until Thu Oct 09 21:53:28 CEST 2014

Export the X509_USER_PROXY variable to the environment (so it can be used by python), **use proxy location in the previous step**::

    export X509_USER_PROXY=/tmp/x509up_uXXXX

This is a one line command for all this procedure::

    export X509_USER_PROXY=$(voms-proxy-init -voms cms | grep Created | cut -c18- | tr -d '.')


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

assign.py
~~~~~~~~~

.. automodule:: WmAgentScripts.assign

.. program-output:: python ../WmAgentScripts/assign.py -h

.. Note::

   - You can provided the -l LFN parameter, be careful to assign the proper one MergeLFN. By default the script will assign the lfn from que original request asociated, in case of ACDCs.
   - You can use this script to assign any kind of workflow.
   - You can use a text file to assign multiple workflows at the same time.
   - You may use additional options to:
      * enforce disk replica subscription
      * change dashboard activity
      * change processing version
      * fix a processing string or acquisition era
      * You can also provide a list of sites separated by commas (no spaces) T1_US_FNAL,T2_US_UCSD,...
      * You can use -s acdc: It will assign to the sites taken from the ACDC server.
      * You can use -s all: It will assign to all sites available (Works for any taskchain acdc).
      * You can skip -s option: It will assign to the "good site" list (Works for any clone you need).


changePriorityWorkflow.py
~~~~~~~~~

.. automodule:: WmAgentScripts.changePriorityWorkflow

.. program-output:: python ../WmAgentScripts/changePriorityWorkflow.py -h


changeSplittingWorkflow.py
~~~~~~~~~~~~~~~~~~~~~~~~~~

This script allows to change the splitting of a request, on a given task name

.. automodule:: WmAgentScripts.changeSplittingWorkflow

.. program-output:: python ../WmAgentScripts/changeSplittingWorkflow.py -h

.. Note::
   - The TASKPATH should be the full task path in which you want to change the splitting, i.e. StepOneProc, StepOne /StepOneProcMerge, Production, etc.
   - The TYPE is the algorithm for splitting.


forceCompleteWorkflows.py
~~~~~~~~~~~~~~~~~~~~~~~~~

Moves a workflow or list of workflows from running-closed to force-completed. This causes every production job to be aborted leaving only log-collect jobs and cleanups

.. automodule:: WmAgentScripts.forceCompleteWorkflows

.. program-output:: python ../WmAgentScripts/forceCompleteWorkflows.py -h


makeACDC.py
~~~~~~~~~

.. automodule:: WmAgentScripts.makeACDC

.. program-output:: python ../WmAgentScripts/makeACDC.py -h

.. Note::
   - Before creation, ACDC documents should be already in couch (usually, it happens when the workflow is completed).
   - If you want to create all possible ACDCs given a workflow, add the option --all.
   - If you need to create an ACDC for an specifc task, you need to have the full task path (not just the last part), i.e. for a workflow with StepOneProc and StepTwoProc:
   - If you want to create an ACDC on StepTwo the taskname is StepOneProc /StepOneProcMerge/StepTwoProc* (or something similar).

recoverMissingLumis.py
~~~~~~~~~

For recovering a list of missing lumis on a workflow with input dataset. For detailed information please go here https://twiki.cern.ch/twiki/bin/view/CMS/CompOpsPRWorkflowTrafficController#Recovering_Workflows

.. program-output:: python ../WmAgentScripts/recoverMissingLumis.py -h


reject.py
~~~~~~~~~

The script allows us to reject or abort (regarding its state) a workflow, or a set of them

.. program-output:: python ../WmAgentScripts/reject.py -h


reqMgrClient.py
~~~~~~~~~~~~~~~

.. automodule:: WmAgentScripts.Unified.reqMgrClient

.. program-output:: python ../WmAgentScripts/Unified/reqMgrClient.py -h


resubmit.py
~~~~~~~~~

This script clones and resubmits a workflow lying either in production or testbed. Be careful with this one, it is being used by reject.py

.. program-output:: python ../WmAgentScripts/resubmit.py -h

.. Note::
   - The workflow is created but NOT assigned, if you need to get it running, follow the instructions here: assign.py
   - When you use the -b option at the end, the script will add the particle "Backfill" to the requestString, AcquisitionEra, Campaing and ProcessingString, so it can be correctly identified as backfill.

