#!/bin/bash

rm -rf tmp
mkdir tmp
cd tmp
dolt init
dolt sql < ../schemas_obf.sql
dolt sql < ../exp_setup_queries_obf.sql
dolt sql < ../build_in_queries_obf.sql
