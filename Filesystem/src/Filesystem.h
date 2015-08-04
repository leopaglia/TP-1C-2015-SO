#ifndef __FILESYSTEM_H__
#define __FILESYSTEM_H__

#include <commons/collections/list.h>
#include <commons/string.h>
#include "Libs.h"

#define OK "1"
#define ULTIMO_BLOQUE "1"
#define NO_ULTIMO_BLOQUE "0"

#define SOY_FS "1"
#define SET_BLOQUE "0"
#define GET_BLOQUE "1"
#define GET_RESULTADO "2"

#define MB 1048576

#define BLOCKSIZE 20*MB
#define SENDSIZE 20*MB
#define DISPONIBLE 1
#define NO_DISPONIBLE 0

#define OCUPADO 1
#define LIBRE 0

#define OPERATIVO 1
#define NO_OPERATIVO 0

t_list* nodosListOff;
t_list* nodosListOn;
t_list* archivosList;

void* map_bloque;
int map_bloque_size;

sem_t mutexFork;

#endif
