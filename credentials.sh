#talk to mcm
cern-get-sso-cookie -u https://cms-pdmv.cern.ch/mcm/ -o ~/private/prod-cookie.txt --krb

#talk to cmsweb
#export X509_USER_PROXY=$HOME/private/personal/voms_proxy.cert
export X509_USER_PROXY=/tmp/$USER-4hty64k793hj
cat $HOME/private/$USER.txt | voms-proxy-init -voms cms --valid 140:00 -pwstdin

