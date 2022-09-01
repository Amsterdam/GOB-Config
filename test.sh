#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo "Running style checks"
flake8

echo "Running unit tests"
coverage run --data-file=/tmp/.coveragerc --source=./gobconfig -m pytest tests/

echo "Coverage report"
coverage report --data-file=/tmp/.coveragerc --show-missing --fail-under=100
