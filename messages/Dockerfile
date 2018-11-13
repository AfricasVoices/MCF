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
CMD pipenv run python messages.py "$USER" /data/input.json /data/input-coda.csv "$FLOW_NAME" "$VARIABLE_NAME" \
/data/output.json /data/output-coda.csv /data/output-icr.csv /data/output-csv.csv /data/input-segment.csv