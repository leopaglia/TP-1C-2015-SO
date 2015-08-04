/*
 * nodo.h
 *
 *  Created on: 27/4/2015
 *      Author: utnso
 */

#ifndef NODO_H_
#define NODO_H_

	#include <sys/mman.h>
	#include <sys/stat.h>
	#include <sys/types.h>
	#include <pthread.h>

	#include "Libs.h"
	#include <commons/collections/list.h>

	#define GB 1073741824
	#define MB 1048576

	#define BLOCKSIZE 20*MB
	#define FILESIZE 1*GB

	#define REDUCELOCAL "1"

	#define ENVIO_BLOQUE "2"

	#define OK "1"

	typedef struct{
		void* codigoMapper;
		int* codeLength;
		int* idBlock;
		char* tempFilename;
		int* jobSocket;
	}mapThreadParams;

	typedef struct{
		void* codigoReducer;
		int* codeLength;
		t_list* fileList;
		int* jobSocket;
		int* local;
	}reduceThreadParams;

	pthread_mutex_t mutexFork;

	char* ID;
	char* IP_FS;
	char* PUERTO_FS;
	char* DATA_FILESIZE;
	char* ARCHIVO_BIN;
	char* DIR_TEMP;
	char* NODO_NUEVO;
	char* IP_NODO;
	char* PUERTO_NODO;

	char* NODO = "0";
	char* SOY_NODO = "1";
	char* CONEXION = "1";
	char* RECONEXION = "0";

	int socketFS;

	char* map_data; //map del archivo completo

	FILE* DATOS;

#endif /* NODO_H_ */
