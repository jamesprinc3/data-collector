#!/bin/bash

git checkout master
git pull
cd ..
python3 data-collector/ -i 5 -e gdax -v false 2> logs/gdax.log &
python3 data-collector/ -i 5 -e bitfinex -v false 2> logs/bitfinex.log &
current_date_time="`date +%Y%m%d%H%M%S`"
echo "Starting data collector at $(date)";


