#!/bin/sh

echo "Executing $1 as cmst1..."
sudo -u cmst1 /bin/bashs --init-file $1
