#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo() {
   builtin echo -e "$@"
}

export COVERAGE_FILE="/tmp/.coverage"


echo "Running mypy"
mypy gobconfig

echo "\nRunning unit tests"
coverage run --source=gobconfig -m pytest

echo "Coverage report"
coverage report --fail-under=100

echo "\nCheck if Black finds no potential reformat fixes"
black --check --diff gobconfig

echo "\nCheck for potential import sort"
isort --check --diff --src-path=gobconfig gobconfig

echo "\nRunning Flake8 style checks"
flake8 gobconfig

echo "\nChecks complete"
