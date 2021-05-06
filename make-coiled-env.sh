#!/bin/bash

poetry export --without-hashes -E docker > requirements.txt
echo "git+https://github.com/gjoseph92/distributed-pyspy.git" >> requirements.txt
coiled env create -n pyspy --pip requirements.txt --private
rm requirements.txt
