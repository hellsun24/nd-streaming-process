#!/bin/bash
set -ue

print_help() {
    echo "Usage: build-image.sh [options]"
    echo "Options:"
    echo " -t <docker-tag-name> - Docker tag name. Default $docker_tag"
    echo " -h                   - Print this help"
}

# parse args
while getopts ":t:k:h" opt; do
  case $opt in
    t)
      docker_tag=$OPTARG
      ;;
    h)
      print_help
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument"
      exit 1
  esac
done

# build docker image
sudo="sudo"
if docker info &> /dev/null; then sudo=""; fi # skip sudo if possible
$sudo docker build -t $docker_tag .


