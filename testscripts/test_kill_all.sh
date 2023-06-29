#!/bin/bash

target=$1
echo "Target name: $target"

output=$(docker ps --format "{{.Names}}" | grep "$target")

containerNames=()

while IFS= read -r line; do
  if [ "$line" != "" ]; then
    containerNames+=("$line")
  fi
done <<< "$output"

if [ ${#containerNames[@]} -eq 0 ]; then
  echo "No services were found for the given target name"
  exit 0
fi

for containerName in "${containerNames[@]}"
do
  echo "Stopping service: ${containerName}"
  docker stop "$containerName"
done

echo "All services were stopped correctly!"