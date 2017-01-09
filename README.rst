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

We can show off the power of autodocumenting all of the members here, since reqmgr.py has lots of documented members.

.. automodule:: WmAgentScripts.reqmgr
   :members:

And So On
---------

I don't know which of these scripts are actually still in use, so someone else should write this or at least make a list of scripts still used.
