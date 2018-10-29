#!/bin/bash

set -e

IMAGE_NAME=mcf-messages

# Check that the correct number of arguments were provided.
if [ $# -ne 9 ]; then
    echo "Usage: sh docker-run.sh <user> <json-input-path> <prev-coda-input-path> <flow-name> <variable-name> <json-output-path> <coda-output-path> <icr-output-path> <csv-output-path>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_JSON=$2
INPUT_CODA=$3
FLOW_NAME=$4
VARIABLE_NAME=$5
OUTPUT_JSON=$6
OUTPUT_CODA=$7
OUTPUT_ICR=$8
OUTPUT_CSV=$9

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
container="$(docker container create --env USER="$USER" --env FLOW_NAME="$FLOW_NAME" --env VARIABLE_NAME="$VARIABLE_NAME" "$IMAGE_NAME")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$INPUT_JSON" "$container:/data/input.json"
if [ -e "$INPUT_CODA" ]; then 
    docker cp "$INPUT_CODA" "$container:/data/input-coda.csv"
fi

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"

mkdir -p "$(dirname "$OUTPUT_CODA")"
docker cp "$container:/data/output-coda.csv" "$OUTPUT_CODA"

mkdir -p "$(dirname "$OUTPUT_ICR")"
docker cp "$container:/data/output-icr.csv" "$OUTPUT_ICR"

mkdir -p "$(dirname "$OUTPUT_CSV")"
docker cp "$container:/data/output-csv.csv" "$OUTPUT_CSV"