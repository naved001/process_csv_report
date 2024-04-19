#!/bin/sh
set -xe

# Add deploy key to ssh config
mkdir -p ~/.ssh
if [ ! -e ~/.ssh/config ]; then
    touch ~/.ssh/config
    touch ~/.ssh/id_nonbillable
    echo "
    Host github-nonbillable
        HostName github.com
        IdentityFile ~/.ssh/id_nonbillable
    " > ~/.ssh/config
    echo "$GH_NONBILLABLE_DEPLOYKEY" > ~/.ssh/id_nonbillable
    chmod 600 ~/.ssh/id_nonbillable
fi

if [ ! -d ./non-billable-projects ]; then
    git clone git@github-nonbillable:CCI-MOC/non-billable-projects.git ./non-billable-projects
fi
