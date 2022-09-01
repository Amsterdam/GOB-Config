# syntax=docker/dockerfile:1
FROM amsterdam/gob_baseimage:3.9-buster
MAINTAINER datapunt@amsterdam.nl

# Install gobconfig in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobconfig module.
COPY gobconfig gobconfig

# Copy test module and tests.
COPY test.sh .flake8 ./
COPY tests tests

USER datapunt
