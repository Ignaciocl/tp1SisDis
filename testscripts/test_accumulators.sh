#!/bin/bash

servicesToStop=( "accumulator-stations2" "accumulator-montreal3" "accumulator-weather3" "accumulator-montreal2" )

while true; do
  for serviceName in "${servicesToStop[@]}"
  do
    echo "Stopping service: ${serviceName}"
    docker stop serviceName
  done
  echo "All services were stopped correctly!"
  sleep 20
done