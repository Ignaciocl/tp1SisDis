#!/bin/bash

servicesToStop=( "worker-station1" "worker-trip1" "worker-weather1")

for serviceName in "${servicesToStop[@]}"
do
  echo "Stopping service: ${serviceName}"
  docker stop serviceName
done

echo "All services were stopped correctly!"