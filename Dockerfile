# syntax=docker/dockerfile:1
FROM python:3.9-bullseye

# Add user datapunt
RUN useradd --user-group --system datapunt

# Copy gobconfig to /app folder.
WORKDIR /app
COPY gobconfig gobconfig

# Install required Python test packages.
COPY pyproject.toml .
RUN pip install --no-cache-dir .[test]

# Copy test module and tests.
COPY test.sh .
COPY tests tests

USER datapunt
