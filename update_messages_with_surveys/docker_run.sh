#!/bin/bash

set -e

IMAGE_NAME=mcf-update-messages-with-surveys

# Check that the correct number of arguments were provided.
if [ $# -ne 11 ]; then
    echo "Usage: sh docker-run.sh <user> <messages-input-file> <feedback-input-file> <demog-survey-input-file> <baseline-survey-input-file> 
    <event-date-survey-input-file> <event-time-survey-input-file> <event-name-survey-input-file> <speaker-question-survey-input-file>
    <endline-survey-input-file> <output-file>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_MESSAGES=$2
INPUT_FEEDBACK=$3
INPUT_DEMOG_SURVEY=$4
INPUT_BASELINE_SURVEY=$5
INPUT_EVENT_DATE_SURVEY=$6
INPUT_EVENT_TIME_SURVEY=$7
INPUT_EVENT_NAME_SURVEY=$8
INPUT_SPEAKER_QUESTION_SURVEY=$9
INPUT_ENDLINE_SURVEY=${10}
OUTPUT_JSON=${11}

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
container="$(docker container create --env USER="$USER" "$IMAGE_NAME")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$INPUT_MESSAGES" "$container:/data/messages-input.json"
docker cp "$INPUT_FEEDBACK" "$container:/data/feedback-input.json"
docker cp "$INPUT_DEMOG_SURVEY" "$container:/data/demog-survey-input.json"
docker cp "$INPUT_BASELINE_SURVEY" "$container:/data/baseline-survey-input.json"
docker cp "$INPUT_EVENT_DATE_SURVEY" "$container:/data/event-date-survey-input.json"
docker cp "$INPUT_EVENT_TIME_SURVEY" "$container:/data/event-time-survey-input.json"
docker cp "$INPUT_EVENT_NAME_SURVEY" "$container:/data/event-name-survey-input.json"
docker cp "$INPUT_SPEAKER_QUESTION_SURVEY" "$container:/data/speaker-question-survey-input.json"
docker cp "$INPUT_ENDLINE_SURVEY" "$container:/data/endline-survey-input.json"

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"