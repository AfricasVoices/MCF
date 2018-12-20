FROM python:3.6-slim

# Install the tools we need.
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile.lock /app
ADD Pipfile /app
RUN pipenv sync

# Make a directory for intermediate data
RUN mkdir /data

# Copy the rest of the project
ADD . /app

# USER is an environment variable which needs to be set when constructing this container e.g. via
# docker run or docker container create. Use docker-run.sh to set these automatically.
CMD pipenv run python update_messages_with_surveys.py "$USER" /data/messages-input.json /data/feedback-input.json /data/demog-survey-input.json \
/data/baseline-survey-input.json /data/event-date-survey-input.json /data/event-time-survey-input.json /data/event-name-survey-input.json \
/data/speaker-question-survey-input.json /data/endline-survey-input.json /data/output.json
