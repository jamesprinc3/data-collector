#!/bin/bash

git checkout master
git pull
cd ..
python3 data-collector/ 2> logs/feed.log &
