#!/bin/bash

echo "----- starting virtualenv -----"
sudo -H pip install virtualenv
virtualenv ./.venv
echo "----- finished virtualenv -----"

echo "----- activating virtualenv and install pip-----"
. ./.venv/bin/activate
pip install -t vendored/ -r requirements.txt
echo "----- activated virtualenv and installed pip -----"
