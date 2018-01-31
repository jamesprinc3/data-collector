# data-collector

## Docker

To build and image named data-collector and launch inside a container named data-collector using docker run the following command from this directory:

`docker build -t data-collector . && docker run --name data-collector data-collector`

To kill the docker container use:

`docker stop data-collector && docker rm -f data-collector`

This allows us to re-run the first command without any issues 

## Output

By deafult, this program will write parquet files to parquet/[EXCHANGE]/orderbook/feed/[DATETIME].parquet every 10 minutes. This behaviour can be customised by editing the Dockerfile to supply some arguments 
