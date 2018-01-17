#
# Python Dockerfile
#
# https://github.com/dockerfile/python
#

# Pull base image.
FROM ubuntu

# Install Python.
RUN apt-get update
RUN apt-get install -y python3 python3-dev python3-pip python3-virtualenv
RUN rm -rf /var/lib/apt/lists/*

COPY requirements.txt /data-collector/

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r /data-collector/requirements.txt

COPY *.py /data-collector/

RUN mkdir /logs

# Define working directory.
WORKDIR /

# Define default command.
#ENTRYPOINT pwd && ls -la && ls -la /
ENTRYPOINT python3 data-collector/
