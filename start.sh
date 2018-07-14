#!/bin/bash
set -e

/code/wait-for-it.sh -t 0 oracle:8080 -- echo "Oracle is up"
mvn -X verify
