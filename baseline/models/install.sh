#!/bin/bash
pip install --upgrade pip
pip install -r requirements.txt

# This runs your wrapped command
"$@"