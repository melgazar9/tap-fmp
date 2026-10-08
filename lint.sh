#!/usr/bin/env bash

set -e

echo "Running black..."
uv run black .
echo "Running flake8..."
uv run flake8
