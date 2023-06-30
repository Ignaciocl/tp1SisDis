#!/bin/bash

servicesToStop=( "worker-station2" "joiner-stations3" "server" "accumulator-stations2" )


while true; do
  for serviceName in "${servicesToStop[@]}"
  do
    echo "Stopping service: ${serviceName}"
    docker stop serviceName
  done
  echo "All services were stopped correctly!"
  sleep 20
done