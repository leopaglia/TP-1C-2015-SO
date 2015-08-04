#!/bin/bash

cat - | awk -F ',' '{print $2 ";" $1  ";" $13 ";" $3}'
	
