#!/bin/bash
# sqlcat.sh — SQL With Claws

PORTER_URL=${PORTER_URL:-http://localhost:32010}

quote=$(porter query "SELECT quote FROM quotes USING SAMPLE 1;" --json \
  | jq -r '.[0].quote')

echo "$quote" | ./kittysay
