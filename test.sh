#!/bin/bash
#script para test ReduceMapFast
#bash test.sh p1 p2 p3

#ejecuto consola Filesystem
x-terminal-emulator -e 'bash exec_fs.sh' &
sleep 1

#modifico config.cfg, abro consola Nodo1
x-terminal-emulator -e 'bash exec_nodo.sh ../src/config.cfg' &
sleep 1

#modifico config.cfg, abro consola Nodo2
x-terminal-emulator -e 'bash exec_nodo.sh ../src/config2.cfg' &
sleep 1

#modifico config.cfg, abro consola Nodo3
x-terminal-emulator -e 'bash exec_nodo.sh ../src/config3.cfg' &
sleep 1

x-terminal-emulator -e 'bash exec_nodo.sh ../src/config4.cfg' &
sleep 1

