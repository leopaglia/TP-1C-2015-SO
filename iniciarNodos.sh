#!/bin/bash

rm -rf Nodo/nodo1
rm -rf Nodo/nodo2
rm -rf Nodo/nodo3
rm -rf Nodo/nodo4

mkdir -p Nodo/nodo1/tmp
mkdir -p Nodo/nodo2/tmp
mkdir -p Nodo/nodo3/tmp
mkdir -p Nodo/nodo4/tmp

truncate -s 1717986918 Nodo/nodo1/data.bin
truncate -s 1717986918 Nodo/nodo2/data.bin
truncate -s 1717986918 Nodo/nodo3/data.bin
truncate -s 1717986918 Nodo/nodo4/data.bin
