export JIRA_SSO_COOKIE=/tmp/$USER-$HOSTNAME-vsdf43vfs
cern-get-sso-cookie -u https://its.cern.ch/jira/loginCern.jsp -o $JIRA_SSO_COOKIE --krb

export MCM_SSO_COOKIE=/tmp/$USER-$HOSTNAME-vfsvdka573t
cern-get-sso-cookie -u https://cms-pdmv.cern.ch/mcm/ -o $MCM_SSO_COOKIE --krb

export X509_USER_PROXY=/tmp/$USER-4hty64k793hj
cat $HOME/private/$USER.txt | voms-proxy-init -voms cms --valid 140:00 --rfc -pwstdin

