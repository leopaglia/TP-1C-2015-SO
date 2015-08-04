/*
 ============================================================================
 Name        : Job.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "Libs.h"

//ver como usar la ruta relativa
#define CONFIG_PATH "/home/utnso/tp-2015-1c-souvenir-jackitos/Job/src/config.cfg"
#define BUFFERSIZE 2048

#define JOB "0"

#define CREA_MAP 0
#define CREA_REDUCE 1
#define TERMINADO 2
#define COPIAR_ARCHIVOS 3
#define REDUCE_FINAL 4
#define COPIAR_ARCHIVOS_COMBINER 5
#define ABORTAR 6

#define CONEXION "0"
#define ENDMAP "1"
#define ENDREDUCE "2"
#define ENDCOPYFILES "3"
#define ENDCOPYFILES_SIN_COMBINER "4"

#define REDUCELOCAL "1"

#define MENSAJE_DESDE_JOB "2"

char* IP_MARTA;
char* PUERTO_MARTA;
char* COMBINER;
char* LISTAARCHIVOS;
char* RUTAMAP;
char* RUTAREDUCE;
char* RUTARESULTADO;

char* bufferDatosDeMarta;

pthread_mutex_t mutexMensaje;

t_log* loggerJob;
int socketMarta;
int isCombiner;

typedef struct {
	char* ip_nodo;
	char* puerto_nodo;
	char* id_nodo;
	char* numero_bloque;
	char* resultFileName;
} datos_map;

typedef struct {
	char* ip_nodo;
	char* puerto_nodo;
	char* id_nodo;
	char* lista_archivos;
	char* resultFileName;
	int local;
} datos_reduce;


int map(datos_map* args){

	char* ip_nodo = string_new();
	char* puerto_nodo = string_new();
	char* id_nodo = string_new();
	char* numero_bloque = string_new();
	char* resultFileName = string_new();

	string_append(&ip_nodo, args->ip_nodo);
	string_append(&puerto_nodo, args->puerto_nodo);
	string_append(&id_nodo, args->id_nodo);
	string_append(&numero_bloque, args->numero_bloque);
	string_append(&resultFileName, args->resultFileName);


	printf("Se crea un hilo Mapper al nodo %s, bloque %s\n", id_nodo, numero_bloque);
	log_info(loggerJob, "Se crea un hilo Mapper al nodo %s, bloque %s\n", id_nodo, numero_bloque);

	int socketNodo = conectar(ip_nodo, puerto_nodo);
//	int socketMartaHilo = conectar(IP_MARTA, PUERTO_MARTA);

	if (socketNodo == -1) return -1;

	char* mensaje_nodo = string_new();

	struct stat stats;
	if(stat(RUTAMAP, &stats) < 0) perror("stat leercodigo");

	FILE* mapFile = fopen(RUTAMAP ,"rb");
	if(mapFile == NULL) perror("Archivo map");

	void* codigo = malloc((int)stats.st_size);

	fread(codigo, (int)stats.st_size, 1, mapFile);

	//string_append(&mensaje_nodo, MENSAJE_DESDE_JOB);
	string_append(&mensaje_nodo, "0");
	string_append(&mensaje_nodo, numero_bloque);
	string_append(&mensaje_nodo, ",");
	string_append(&mensaje_nodo, resultFileName);
	string_append(&mensaje_nodo, ",");
	string_append_with_format(&mensaje_nodo, "%d", (int)stats.st_size);


	sendHeader(MENSAJE_DESDE_JOB, strlen(mensaje_nodo), socketNodo);
	enviar(socketNodo, mensaje_nodo, strlen(mensaje_nodo));
	enviar(socketNodo, codigo, (int)stats.st_size);


	//aca deberia esperar la respuesta del nodo
	char* bufferRespuestaNodo = calloc(3, 1);
	recibir(socketNodo, bufferRespuestaNodo, 3);

	if(bufferRespuestaNodo[0] == '\0') string_append(&bufferRespuestaNodo, "-1");

	//le envio la respuesta a Marta
	char* response = string_new();

	//string_append(&response, JOB);
	string_append(&response, ENDMAP);
	string_append(&response, id_nodo);
	string_append(&response, ",");
	string_append(&response, numero_bloque);
	string_append(&response, ",");
	string_append(&response, bufferRespuestaNodo); //resultado del map


//	printf("RESPUESTA DEL NODO MAP-> %s\n", bufferRespuestaNodo);
//	printf("MENSAJE A MARTITA MAP-> %s\n", response);

	pthread_mutex_lock(&mutexMensaje);

	sendHeader(MENSAJE_DESDE_JOB, strlen(response), socketMarta);
	enviar(socketMarta,response, strlen(response));

    pthread_mutex_unlock(&mutexMensaje);

	printf("Termino el hilo Mapper ID: %s, Bloque %s\n", id_nodo, numero_bloque);
	log_info(loggerJob, "Termino el hilo Mapper ID: %s, Bloque %s\n", id_nodo, numero_bloque);

	return 0;
};

int reduce(datos_reduce* args){

	char* ip_nodo = string_new();
	char* puerto_nodo = string_new();
	char* lista_archivos = string_new();

	string_append(&ip_nodo, args->ip_nodo);
	string_append(&puerto_nodo, args->puerto_nodo);
	string_append(&lista_archivos, args->lista_archivos);

	int socketNodo = conectar(ip_nodo, puerto_nodo);

	if (socketNodo == -1) return -1;

	printf("Se crea un hilo Reduce al nodo %s - %s\n", ip_nodo, puerto_nodo);
	log_info(loggerJob, "Se crea un hilo Mapper con los siguientes parametros :%s - %s - %s\n", ip_nodo, puerto_nodo, lista_archivos);
	printf("Job se conecto a Nodo con IP:%s y Puerto: %s \n", ip_nodo, puerto_nodo);
	//log_info(loggerJob, "Job se conecto a Nodo con IP:%s y Puerto: %s \n", ip_nodo, puerto_nodo);

	char* mensaje_nodo = string_new();

	struct stat stats;
	if(stat(RUTAREDUCE, &stats) < 0) perror("stat leercodigo");

	FILE* reduceFile = fopen(RUTAREDUCE ,"rb");
	if(reduceFile == NULL) perror("Archivo map");

	void* codigo = malloc((int)stats.st_size);

	fread(codigo, (int)stats.st_size, 1, reduceFile);

	//[RUTA DEL MENSAJE][RUTA_PROGRAMA, RUTA_RESULTADO]
//	string_append(&mensaje_nodo, MENSAJE_DESDE_JOB);
	string_append(&mensaje_nodo, "1");
	string_append(&mensaje_nodo, lista_archivos);
	string_append(&mensaje_nodo, ",");
	string_append_with_format(&mensaje_nodo, "%d", (int)stats.st_size);


	sendHeader(MENSAJE_DESDE_JOB, strlen(mensaje_nodo), socketNodo);
	enviar(socketNodo, mensaje_nodo, strlen(mensaje_nodo));
	enviar(socketNodo, codigo, (int)stats.st_size);


	//aca deberia esperar la respuesta del nodo
	char bufferRespuestaNodo[BUFFERSIZE];
	recibir(socketNodo, bufferRespuestaNodo, BUFFERSIZE);

	//le envio la respuesta a Marta.
	char* response = string_new();

	string_append(&response, ENDREDUCE);
	bufferRespuestaNodo != NULL ? string_append(&response, bufferRespuestaNodo) : string_append(&response, "-1"); // id,resultado, nombrearchivo
	string_append(&response, ",");
	string_append(&response, string_itoa(args->local));

//	printf("RESPUESTA DEL NODO REDUCE-> %s\n", bufferRespuestaNodo);
//	printf("MENSAJE A MARTITA REDUCE-> %s\n", response);

	pthread_mutex_lock(&mutexMensaje);

	sendHeader(MENSAJE_DESDE_JOB, strlen(response), socketMarta);
	enviar(socketMarta,response, strlen(response));

    pthread_mutex_unlock(&mutexMensaje);

    printf("Termino el hilo Reduce \n");
	log_info(loggerJob, "Termino el hilo Mapper por %s", response);

	return 0;
};

int main(int argc, char** argv) {

	pthread_mutex_init(&mutexMensaje, NULL);

	pthread_t map_thread;
	pthread_t reduce_thread;
	loggerJob = log_create("Job.log", "Job", 0, LOG_LEVEL_INFO);

	char* propiedades[7] = {"IP_MARTA", "PUERTO_MARTA", "COMBINER", "ARCHIVOS", "MAPPER", "REDUCE", "RESULTADO"};
	char** variables[7] = {&IP_MARTA, &PUERTO_MARTA, &COMBINER, &LISTAARCHIVOS, &RUTAMAP, &RUTAREDUCE, &RUTARESULTADO};

	inicializarStrings(variables, 7);

	leerConfig(argv[1], propiedades, variables, 7);

	socketMarta = conectar(IP_MARTA, PUERTO_MARTA);
	printf("Job se conecto a Marta con IP:%s y Puerto: %s \n", IP_MARTA, PUERTO_MARTA);
	log_info(loggerJob, "Job se conecto a Marta con IP:%s y Puerto: %s \n", IP_MARTA, PUERTO_MARTA);

	//si le van a poner SI y NO en el config, si lo cambian a 1 y 0 no hace falta
	char* combiner = string_new();
	if(strcmp(COMBINER, "NO") == 0)
	{
		string_append(&combiner, "0");
		isCombiner=0;
	}
	else if (strcmp(COMBINER, "SI") == 0)
	{
		string_append(&combiner, "1");
		isCombiner=1;
	}


	char* mensaje = string_new();

	//string_append(&mensaje, JOB);
	string_append(&mensaje, CONEXION);
	string_append(&mensaje, combiner);
	string_append(&mensaje, LISTAARCHIVOS);
	string_append(&mensaje, ",");
	string_append(&mensaje, RUTARESULTADO);

	sendHeader(MENSAJE_DESDE_JOB, strlen(mensaje), socketMarta);

	enviar(socketMarta, mensaje, strlen(mensaje));

	char* bufferHeader;
	char* bufferHeader2;

	while(1){

		int flag = 0;
		bufferHeader = calloc(HEADERSIZE+1,1);
		bufferHeader2 = calloc(HEADERSIZE+2, 1);

		recibir(socketMarta, bufferHeader, HEADERSIZE);

		if(atoi(bufferHeader) == 9999){ //la bardeada en marta (si le llega 0000, necesita un buffer de 5), es una cagada pero funciona
			recibir(socketMarta, bufferHeader2, HEADERSIZE+1);
			flag = 1;
		}

		char* msj1 = string_new();
		if(flag == 0){
			msj1 = string_substring_from(bufferHeader, 1);
		}else{
			msj1 = string_substring_from(bufferHeader2, 1);
		}

		int tamARecibir = atoi(msj1);

//		printf("Cantidad a recibir = %d\n", tamARecibir);


		bufferDatosDeMarta = calloc(tamARecibir + 1, 1);



		int n = 0;
		int s = 0;
		while( (s != tamARecibir) && ( (n = recibir(socketMarta, bufferDatosDeMarta + s, tamARecibir - s))  > 0 ) ){
		  	s = s + n;
		}

//		char* bufferRealoc = string_new();
//		string_append(&bufferRealoc, string_substring(bufferDatosDeMarta, 0, atoi(msj1)));

//		printf("LARGO BUFFER RECIBIDO: %d \n", string_length(bufferDatosDeMarta));
//		if(flag == 0){
//			printf("BUFFER RECIBIDO: %s \n", bufferDatosDeMarta);
//		}

		char** datosMartaPedidos = string_split(bufferDatosDeMarta, "*");

		if(strtol(datosMartaPedidos[0], NULL, 10) == ABORTAR){
			printf("Job termina sin exito. \n");
			exit(0);
		}

		if(strtol(datosMartaPedidos[0], NULL, 10) == COPIAR_ARCHIVOS){
//			mensaje nuevo:COPIAR_ARCHIVOS*ip, puerto,cantidadarchivos-arch3-arch5#ip, puerto,cantidadarchivos-arch3-arch4

			char* mensaje = datosMartaPedidos[1];

			char** mensaje_split = string_split(mensaje, "_");

			char** datosNodosConArchivos = string_split(mensaje_split[0], "#");
			char** datosNodoReceptor = string_split(mensaje_split[1], ",");

//			printf("\nDatos recibidos: %s \n\n", mensaje_split[0]);

			char* ipReceptor = datosNodoReceptor[0];
			char* puertoReceptor = datosNodoReceptor[1];

			printf("Nodo que va a hacer el reduce final - ip: %s puerto: %s\n",ipReceptor, puertoReceptor);

			int x = 0;
			bool huboFalla = false;
			int socketNodoReceptor = conectar(ipReceptor, puertoReceptor);
			while (datosNodosConArchivos[x] != '\0'){

				char* mensaje_archivos_copiar = string_new();
				string_append(&mensaje_archivos_copiar, "2");
				string_append(&mensaje_archivos_copiar, datosNodosConArchivos[x]);

				sendHeader(MENSAJE_DESDE_JOB, strlen(mensaje_archivos_copiar), socketNodoReceptor);
				enviar(socketNodoReceptor, mensaje_archivos_copiar, strlen(mensaje_archivos_copiar));

				x++;
			}

			//le envio la respuesta a Marta
			char* mensaje_respuesta_marta = string_new();
			if(strcmp(COMBINER, "NO") != 0)
			{
				string_append(&mensaje_respuesta_marta, ENDCOPYFILES);
			}
			else
			{
				string_append(&mensaje_respuesta_marta, ENDCOPYFILES_SIN_COMBINER);
				string_append(&mensaje_respuesta_marta, ipReceptor);
				string_append(&mensaje_respuesta_marta, ",");
				string_append(&mensaje_respuesta_marta, puertoReceptor);
			}

//			printf("MENSAJE A MARTITA COPIAR_ARCHIVOS-> %s\n", mensaje_respuesta_marta);
//			string_append(&mensaje_respuesta_marta, "*");
//			string_append(&mensaje_respuesta_marta, string_itoa(huboFalla));
			sendHeader(MENSAJE_DESDE_JOB, strlen(mensaje_respuesta_marta), socketMarta);
			enviar(socketMarta,mensaje_respuesta_marta, strlen(mensaje_respuesta_marta));

			continue;
		}



		if(strtol(datosMartaPedidos[0], NULL, 10) == CREA_REDUCE){

			char** datosNodos = string_split(datosMartaPedidos[1], "_");

			int x = 0;
			while (datosNodos[x] != '\0')
			{
				datos_reduce* reduceArgs = malloc(sizeof(datos_reduce));

				char** datosReduce = string_split(datosNodos[x], ",");

				reduceArgs->id_nodo = datosReduce[0];
				reduceArgs->ip_nodo = datosReduce[1];
				reduceArgs->puerto_nodo = datosReduce[2];
				reduceArgs->lista_archivos = datosReduce[3];
//					reduceArgs->resultFileName = datosReduce[3];
				reduceArgs->local = 1;
//					reduceArgs->resultFileName = RUTARESULTADO;

				pthread_create(&reduce_thread, NULL, (void*) reduce, (void*) reduceArgs);
//				reduce(reduceArgs);
				x++;
			}

			continue;

		}

		if(strtol(datosMartaPedidos[0], NULL, 10) == REDUCE_FINAL){
			//Mensaje:REDUCE_FINAL*ip,puerto,arch133-arch656

			char** datosReduce = string_split(datosMartaPedidos[1], ",");

			datos_reduce* reduceArgs = malloc(sizeof(datos_reduce));

			reduceArgs->ip_nodo = datosReduce[0];
			reduceArgs->puerto_nodo = datosReduce[1];
			reduceArgs->lista_archivos = datosReduce[2];
			reduceArgs->local = 0;

			pthread_create(&reduce_thread, NULL, (void*) reduce, (void*) reduceArgs);
			pthread_detach(reduce_thread);

//			reduce(reduceArgs);
			continue;
		}

		if(strtol(datosMartaPedidos[0], NULL, 10) == TERMINADO) {
			printf("Job terminado.\n");
			break;
		}



		int t=0;
		while(datosMartaPedidos[t+1] != NULL)
		{

			if(strtol(datosMartaPedidos[0], NULL, 10) == CREA_MAP){

				t++;
				char** datosMap = string_split(datosMartaPedidos[t], ",");

				datos_map* mapArgs = malloc(sizeof(datos_map));

					mapArgs->ip_nodo = datosMap[0];
					mapArgs->puerto_nodo = datosMap[1];
					mapArgs->id_nodo = datosMap[2];
					mapArgs->numero_bloque = datosMap[3];
					mapArgs->resultFileName = datosMap[4];


				pthread_create(&map_thread, NULL, (void*) map, (void*) mapArgs);
				pthread_detach(map_thread);

//				map(mapArgs);

				continue;

			}



		}

		//free(mensajeRecibido);
		free(bufferDatosDeMarta);
	}
	free(bufferDatosDeMarta);
	//free(mapArgs);
	//free(reduceArgs);

	return EXIT_SUCCESS;
}





