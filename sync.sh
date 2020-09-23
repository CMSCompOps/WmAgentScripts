#!/bin/bash

cd /data/unified/WmAgentScripts||exit
git stash
git fetch origin
git rebase origin/master
git stash apply
