#!/bin/bash

# Suppress multiple urllib3 warnings
export PYTHONWARNINGS="ignore:Unverified HTTPS request,ignore::urllib3.exceptions.NotOpenSSLWarning"
echo "Building freeds lab jafkafe simulator..."
poetry lock
poetry version patch
poetry export -f requirements.txt --output requirements.txt --without-hashes
freeds dc down -s .
freeds dc build -s .
freeds dc up -s .
docker logs jafkafe

