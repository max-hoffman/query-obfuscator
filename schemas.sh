#!/bin/bash

dolt sql -q "show tables" -r csv | tail +2 | while IFS= read -r tab; do
    dolt sql -q "show create table $tab" -r json | jq --raw-output '.[] |  .[0]."Create Table"' |  sed 's/utf8mb4_0900_bin/utf8mb4_0900_bin;/g' >> schemas.sql
done
