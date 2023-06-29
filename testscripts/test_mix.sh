#!/bin/bash

servicesToStop=( "worker-station1" "joiner-stations" "server" )

for serviceName in "${servicesToStop[@]}"
do
  echo "Stopping service: ${serviceName}"
  docker stop serviceName
done

echo "All services were stopped correctly!"