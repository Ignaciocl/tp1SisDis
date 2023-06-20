#!/bin/bash

servicesToStop=( "joiner-montreal" "joiner-stations" "joiner-weather" )

for serviceName in "${servicesToStop[@]}"
do
  echo "Stopping service: ${serviceName}"
  docker stop serviceName
done

echo "All services were stopped correctly!"