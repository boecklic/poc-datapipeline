#!/bin/bash

PARAMS=$(cat ogd-local-forecasting_meta_parameters.csv | awk -F ';' {'print $1'} | tail -n +2)

# put current date as yyyy-mm-dd HH:MM:SS in $date
YESTERDAY=$(date -d "yesterday 10:00" '+%Y%m%d')
echo "Downloading data for ${YESTERDAY}"
# echo $PARAMS

mkdir -p ${YESTERDAY}
cd ${YESTERDAY}

for param in $PARAMS;
do
    URL="https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/${YESTERDAY}-ch/vnut12.lssw.${YESTERDAY}1000.${param}.csv"
    echo $URL
    wget $URL
done
cd -
