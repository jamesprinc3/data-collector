#!/bin/bash

git checkout master
git pull
sudo python3 -m pip install -r requirements.txt
cd ..
mkdir logs
python3 data-collector/ -i 600 -e gdax -v false 2> logs/gdax.log &
python3 data-collector/ -i 600 -e bitfinex -v false 2> logs/bitfinex.log &
current_date_time="`date +%Y%m%d%H%M%S`"
echo "Starting data collector at $(date)";


