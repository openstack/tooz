#!/bin/bash

set -e

python_version=$(python --version 2>&1)
echo "Running using '$python_version'"
for filename in examples/*.py; do
    echo "Activating '$filename'"
    python $filename
done
