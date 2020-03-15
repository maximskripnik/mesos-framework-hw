#!/bin/sh

TARGET_URL=$1

DATA=$(head --bytes 100 /dev/random | base64)

curl -v -H 'Content-Type:text/plain' --data '$DATA' $TARGET_URL
