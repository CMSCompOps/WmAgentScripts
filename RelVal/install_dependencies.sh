

#!/bin/bash

DBS3CLIENTVERSION=3.2.10-comp
DBS3CLIENT=cms+dbs3-client+$DBS3CLIENTVERSION

HTTPLIB2VERSION=0.7.1-comp6
HTTPLIB2=external+py2-httplib2+$HTTPLIB2VERSION

MYSQLDBVERSION=1.2.4b4-comp4
MYSQLDB=external+py2-mysqldb+$MYSQLDBVERSION

WORKINGDIR=/home/$USER
SCRAM_ARCH=slc6_amd64_gcc481
REPO=comp.pre
SWAREA=$WORKINGDIR/sw/$REPO

check_success()
{
  if [ $# -ne 2 ]; then
    echo "check_success expects exact two parameters."
    exit 1
  fi

  local step=$1
  local exit_code=$2

  if [ $exit_code -ne 0 ]; then
    echo "$step was not successful"
    exit $exit_code
  fi
}

cleanup_swdir()
{
  rm -rf $SWAREA
  check_success "Cleaning up $SWAREA" $?
}

prepare_bootstrap()
{
  ## prepare bootstrapping
  mkdir -p $SWAREA
  wget -O $SWAREA/bootstrap.sh http://cmsrep.cern.ch/cmssw/cms/bootstrap.sh
  check_success "Preparing bootstrapping" $?
}

bootstrapping()
{
  ## bootstrapping
  chmod +x $SWAREA/bootstrap.sh
  sh -x $SWAREA/bootstrap.sh setup -repository $REPO -path $SWAREA -arch $SCRAM_ARCH >& $SWAREA/bootstrap_$SCRAM_ARCH.log

  check_success "Bootstrapping" $?
}

install_software()
{
  cleanup_swdir
  prepare_bootstrap
  bootstrapping

  ## software installation
  source $SWAREA/$SCRAM_ARCH/external/apt/*/etc/profile.d/init.sh
  apt-get update -y
  apt-get install $DBS3CLIENT -y
  apt-get update -y
  apt-get install $HTTPLIB2 -y
  apt-get update -y
  apt-get install $MYSQLDB -y
  check_success "Install $DBS3CLIENT" $?
  check_success "Install $HTTPLIB2" $?
  check_success "Install $MYSQLDB" $?
}

### run software installation
install_software
