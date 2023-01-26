#!/bin/bash

cd /data/unifiedPy3-fast/WmAgentScripts
git stash
git fetch origin
git rebase origin/master
git stash apply
chmod +x *sh
