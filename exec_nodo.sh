#!/bin/bash

#ejecuto el nodo

cd ./Nodo/Debug

make clean && make

./Nodo $1
