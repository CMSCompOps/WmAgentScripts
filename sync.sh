#!/bin/bash

cd /data/unified/WmAgentScripts
git stash
git fetch origin
git rebase origin/master
git stash apply
