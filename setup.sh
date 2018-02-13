#!/bin/bash

sudo apt-get update
sudo apt-get install -y git
sudo apt-get install -y python3 python3-dev python3-pip python3-virtualenv
sudo apt-get install htop

python3 -m pip install --upgrade pip