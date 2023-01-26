#!/bin/bash

cd /data/unifiedPy3-fast/WmAgentScripts
git stash
git fetch origin
git rebase origin/python3-migration-fast
git stash apply
chmod +x *sh
