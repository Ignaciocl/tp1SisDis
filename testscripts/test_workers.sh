#!/bin/bash

servicesToStop=( "worker-station" "worker-trip" "worker-weather")

if [ $# -eq 0 ]; then
    echo "No ID provided. Please provide client ID as a parameter."
    exit 1
fi

clientID=$1

while true; do
  for serviceName in "${servicesToStop[@]}"
  do
    echo "Stopping service: ${serviceName}${clientID}"
    docker stop "${serviceName}${clientID}"
  done

  echo "All services were stopped correctly!"
  sleep 20
done