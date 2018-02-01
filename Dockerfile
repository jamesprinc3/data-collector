#
# Python Dockerfile
#
# https://github.com/dockerfile/python
#

# Pull base image.
FROM alpine

# Install Python.
RUN apk update
RUN apk add --update python3 python3-dev
RUN rm -rf /var/lib/apt/lists/*

COPY requirements.txt /data-collector/

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install virtualenv
RUN python3 -m pip install -r /data-collector/requirements.txt

COPY *.py /data-collector/

RUN mkdir /logs

# Define working directory.
WORKDIR /

# Define default command.
#ENTRYPOINT pwd && ls -la && ls -la /
ENTRYPOINT python3 data-collector/
