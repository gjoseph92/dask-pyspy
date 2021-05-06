#!/bin/bash

LATEST_COMMIT=$(git rev-parse origin/main)
poetry export --without-hashes -E docker > requirements.txt
echo "git+https://github.com/gjoseph92/distributed-pyspy.git@$LATEST_COMMIT" >> requirements.txt
coiled env create -n pyspy --pip requirements.txt --private
rm requirements.txt
