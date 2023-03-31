# syntax=docker/dockerfile:1
FROM amsterdam/gob_baseimage:3.9-bullseye
MAINTAINER datapunt@amsterdam.nl

# Install gobconfig in /app folder.
WORKDIR /app
COPY gobconfig gobconfig

# Install required Python test packages.
COPY pyproject.toml .
RUN pip install --no-cache-dir --editable .[test]

# Copy test module and tests.
COPY test.sh .
COPY tests tests

USER datapunt
