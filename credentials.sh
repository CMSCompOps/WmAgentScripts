
#talk to mcm
export JIRA_SSO_COOKIE=/tmp/$USER-$HOSTNAME-vsdf43vfs
if [ "$1" == "create" ] ; then
    echo "creating jira cookie $JIRA_SSO_COOKIE"
    cern-get-sso-cookie -u https://its.cern.ch/jira/loginCern.jsp -o "$JIRA_SSO_COOKIE" --krb
else
    echo "using jira cookie $JIRA_SSO_COOKIE"
fi

export MCM_SSO_COOKIE=/tmp/$USER-$HOSTNAME-vfsvdka573t
if [ "$1" == "create" ] ; then
    echo "creating mcm cookie $MCM_SSO_COOKIE"
    cern-get-sso-cookie -u https://cms-pdmv.cern.ch/mcm/ -o "$MCM_SSO_COOKIE" --krb
else
    echo "using mcm cookie $MCM_SSO_COOKIE"
fi

#talk to cmsweb
export X509_USER_PROXY=/tmp/$USER-4hty64k793hj
if [ "$1" == "create" ] ; then
    echo "creating grid proxy00 $X509_USER_PROXY"
    # shellcheck disable=SC2002
    cat "$HOME"/private/"$USER".txt | voms-proxy-init -voms cms --valid 140:00 --rfc -pwstdin
else
    echo "using grid proxy $X509_USER_PROXY"
fi

