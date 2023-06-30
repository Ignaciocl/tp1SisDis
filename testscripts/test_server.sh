#!/bin/bash

while true; do
  echo "Stopping server"
  docker stop server
  echo "Server stopped"
  sleep 20
done