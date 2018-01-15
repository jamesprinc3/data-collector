#!/bin/bash

git checkout master
git pull
cd ..
python3 data-collector/ 2> logs/feed.log &
current_date_time="`date +%Y%m%d%H%M%S`"
echo "Starting data collector at $(date)";
