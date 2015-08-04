/*
 ============================================================================
 Name        : MaRTA.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/collections/list.h>
#include "MaRTA.h"
#include "Libs.h"

//ver como usar la ruta relativa
#define CONFIG_PATH "/home/utnso/tp-2015-1c-souvenir-jackitos/MaRTA/src/config.cfg"

#define FILESYSTEM '1'
#define JOB '2'

#define MAP "0"
#define REDUCE "1"
#define TERMINADO "2"
#define COPIAR_ARCHIVOS "3"
#define REDUCE_FINAL "4"
#define COPIAR_ARCHIVOS_COMBINER "5"
#define ABORTARJOB "6"

#define DESCONEXIONNODO '9'

#define REDUCELOCAL "1"

#define NUEVONODO '1'

#define CONNECT '0'
#define ENDMAP '1'
#define ENDREDUCE '2'
#define ENDCOPYFILES '3'
#define ENDCOPYFILES_SIN_COMBINER '4'

#define TEMPLATE "/mapb"
#define TEMPLATEREDUCE "/mapb"

#define SOY_MARTA "0"
#define PIDO_UBICACION "1"

int socketFS;
char* PUERTO_LISTEN;
char* IP_FS;
char* PUERTO_FS;

//uso un global porque las funciones de lista solo pueden recibir como parametro un elemento de la lista
int auxGlobalSocket; // tiene el socket del ultimo job que paso por la funcion connect

t_list* jobsList;
t_list* nodosList;

typedef struct{
	int* id;
	char* ip;
	char* puerto;
	int* carga; //cantidad de tareas ejecutandose
} t_nodo ;

typedef struct{
	int idNodo;
	char* nombreArchivo;
	int terminado;
	int cantArchivos;
	int isCopyFinal;
} t_reduce ;

typedef struct{
	int* socket;
	t_list* maps;
	t_list* reduces;
	int* combiner;
	char* rutaResultado;
} t_job ;

typedef struct{
	int* idNodo;
	int* idBloque;
	int* idNodoOpcion2;
	int* idBloqueOpcion2;
	int* idNodoOpcion3;
	int* idBloqueOpcion3;
	int* terminado;
} t_map ;


void replanificarMapFallado(t_job* job, t_map* map){

	int idNodoNuevo = *(map->idNodoOpcion2);
	int idBloqueNuevo = *(map->idBloqueOpcion2);

	bool by_idNodo(t_nodo* n) {
		return (*(n->id) == idNodoNuevo);
	}
	t_nodo* nodo = list_find(nodosList, (void*)by_idNodo);

	//guardo valores anteriores
	int idNodoViejo = *(map->idNodo);
	int idBloqueViejo = *(map->idBloque);

	//actualizo el map
	*(map->idNodo) = idNodoNuevo;
	*(map->idBloque) = idBloqueNuevo;
	list_add(job->maps, map);

	char* mensajeAJob = string_new();
	string_append(&mensajeAJob, MAP);
	string_append(&mensajeAJob, "*");

	char* resultFileName = string_new();
	string_append(&resultFileName, TEMPLATE);

	string_append(&mensajeAJob, nodo->ip);
	string_append(&mensajeAJob, ",");
	string_append(&mensajeAJob, nodo->puerto);
	string_append(&mensajeAJob, ",");
	string_append(&mensajeAJob, string_itoa(idNodoNuevo));
	string_append(&mensajeAJob, ",");
	string_append(&mensajeAJob, string_itoa(idBloqueNuevo));
	string_append(&mensajeAJob, ",");
	string_append(&mensajeAJob, string_duplicate(resultFileName));
	string_append(&mensajeAJob, string_itoa(idNodoNuevo));
	string_append(&mensajeAJob, string_itoa(idBloqueNuevo));


	if(strlen(mensajeAJob) > 999){
		char* nuevoheader = string_new();
		string_append(&nuevoheader, SOY_MARTA); //1 digito
		string_append(&nuevoheader, string_itoa(strlen(mensajeAJob)));//4 digitos

		enviar(*(job->socket), "9999", 4); //falso header
		enviar(*(job->socket), nuevoheader, 5); //header de 5
	}else{
		sendHeader(SOY_MARTA, strlen(mensajeAJob), *(job->socket));
	}
	enviar(*(job->socket), mensajeAJob, strlen(mensajeAJob));

	printf("Replanificacion nodo %d, bloque %d enviada a job.\nNuevo map en nodo %d, bloque %d\n", idNodoViejo, idBloqueViejo, idNodoNuevo, idBloqueNuevo);

}


void replanificarMapsNodo(t_job* job, int idNodo){


	bool _del_nodo_caido(t_map* m){
		return *(m->idNodo) == idNodo;
	}
	t_list* mapsAReplanificar = list_filter(job->maps, (void*)_del_nodo_caido);
//TODO limpiar
	bool _no_del_nodo_caido(t_map* m){
		return *(m->idNodo) != idNodo;
	}
	job->maps = list_filter(job->maps, (void*)_no_del_nodo_caido);

	if(mapsAReplanificar->elements_count != 0)
		printf("\n\n **Inicio replanificacion del nodo %d \n\n", idNodo);

	int i;
	for(i = 0; i < mapsAReplanificar->elements_count; i++){
		t_map* map = list_get(mapsAReplanificar, i);

		replanificarMapFallado(job, map);
	}
	if(mapsAReplanificar->elements_count != 0)
		printf("\n\n **Fin replanificacion del nodo %d \n\n", idNodo);
}


void replanificarJobNuevoNodo(t_job* job, int idNodo, int idBloque) {

	bool by_idNodo(t_nodo* n) {
		return (*(n->id) == idNodo);
	}
	t_nodo* nodo = list_find(nodosList, (void*)by_idNodo);

	if(nodo != NULL){ //si el nodo al que le fallo el map no se desconecto

		bool _by_idNodo_idBloque(t_map* m) {
			return (*(m->idNodo) == *(nodo->id)) && (*(m->idBloque) == idBloque);
		}

		t_map* map = list_remove_by_condition(job -> maps, (void*)_by_idNodo_idBloque);

		replanificarMapFallado(job, map);

	}else{
		replanificarMapsNodo(job, idNodo);
	}

}

bool _globalEstaConectado(t_job* n){
	return auxGlobalSocket == *(n->socket);
}

int jobConectado(){
	return (list_any_satisfy(jobsList, (void*) _globalEstaConectado));
}

void agregarNodo(char* mensaje){

	char** arrayDatosNodo = string_split(mensaje, ",");

	t_nodo* n = malloc(sizeof(t_nodo));
	n->carga = malloc(sizeof(int));
	n->id = malloc(sizeof(int));
	n->ip = string_new();
	n->puerto = string_new();

	*(n->carga) = 0;
	*(n->id) = atoi(arrayDatosNodo[0]);
	string_append(&(n->ip), arrayDatosNodo[1]);
	string_append(&(n->puerto), arrayDatosNodo[2]);

	list_add(nodosList, n);

	printf("Nodo ID %d conectado, IP: %s, PUERTO: %s\n", *(n->id), n->ip, n->puerto);

}

void sacarNodo(char* mensaje){

	int id = atoi(mensaje);

    bool _by_id(t_nodo* n1) {
        return *(n1->id) == id;
    }
	t_nodo* nodo = list_remove_by_condition(nodosList, (void*)_by_id );

	//TODO limpiar memoria nodo

	if(nodo != NULL)
		printf("Nodo ID %d desconectado\n", *(nodo->id));

}

void agregarJob(char combiner, char* rutaResultado){

	t_job* nuevoJob = malloc(sizeof(t_job));

	nuevoJob->socket = malloc(sizeof(int));
	//nuevoJob->reduces = malloc(sizeof(int));
	nuevoJob->combiner = malloc(sizeof(int));
	nuevoJob->rutaResultado = string_new();

	nuevoJob->maps = list_create();
	nuevoJob->reduces = list_create();

	string_append(&(nuevoJob->rutaResultado), rutaResultado);
	*(nuevoJob->socket) = auxGlobalSocket;
	*(nuevoJob->combiner) = combiner;

	list_add(jobsList, nuevoJob);

	printf("Nuevo Job conectado, combiner: %d\n", *(nuevoJob->combiner));

}

int inArray(int num, int* array, int size){
	int i;
	for( i = 0; i < size; i++)
		if(num == array[i]) return i;

	return -1;
}

//ordena los nodos por carga, y va agarrando de a uno hasta que saque uno que este en el array de ids
//tristisimo, buscar una mejor forma
//recibe array de 3 posiciones
int asignarANodoConMenorTrabajo (int* arrayIds, int* arrayBloques){

    bool _menor_carga(t_nodo* n1, t_nodo* n2) {
        return *(n1->carga) <= *(n2->carga);
    }

    list_sort(nodosList, (void*)_menor_carga);

    int pos;

    int i = 0;
    t_nodo* nodoConMenorCarga;
    //espantoso, pensa algo mejor y cambialo
    while(1){
    	nodoConMenorCarga = list_get(nodosList, i);

		if((pos = inArray(*(nodoConMenorCarga->id), arrayIds, 3)) != -1)
			break;

		i++;
    }

    //el job que mando el mensaje
    t_job* job = list_find(jobsList, (void*)_globalEstaConectado);

    t_map* newMap = malloc(sizeof(t_map));
    newMap -> idNodo = malloc(sizeof(int));
    newMap -> idBloque = malloc(sizeof(int));
    newMap -> idNodoOpcion2 = malloc(sizeof(int));
    newMap -> idBloqueOpcion2 = malloc(sizeof(int));
    newMap -> idNodoOpcion3 = malloc(sizeof(int));
    newMap -> idBloqueOpcion3 = malloc(sizeof(int));
    newMap -> terminado = malloc(sizeof(int));

    *(newMap -> idNodo) = *(nodoConMenorCarga->id);
    *(newMap -> idBloque) = arrayBloques[inArray(*(nodoConMenorCarga->id), arrayIds, 3)];//pos

    //Esto estaria mal porque le estoy asignando cualquiera como opcion 2 y 3 sin tener en cuenta la carga
    //Ademas es un asco
    if(pos == 0)
    {
    	*(newMap -> idNodoOpcion2) = arrayIds[1];
    	*(newMap -> idBloqueOpcion2) = arrayBloques[1];
    	*(newMap -> idNodoOpcion3) = arrayIds[2];
    	*(newMap -> idBloqueOpcion3) = arrayBloques[2];
    }
    else
    {
    	if(pos == 1)
    	{
    		*(newMap -> idNodoOpcion2) = arrayIds[0];
    		*(newMap -> idBloqueOpcion2) = arrayBloques[0];
    		*(newMap -> idNodoOpcion3) = arrayIds[2];
    		*(newMap -> idBloqueOpcion3) = arrayBloques[2];
    	}
    	else
    	{
    		*(newMap -> idNodoOpcion2) = arrayIds[0];
    		*(newMap -> idBloqueOpcion2) = arrayBloques[0];
    		*(newMap -> idNodoOpcion3) = arrayIds[1];
    		*(newMap -> idBloqueOpcion3) = arrayBloques[1];

    	}
    }

    *(newMap -> terminado) = 0;

    list_add(job->maps, newMap);

    int carga = *(nodoConMenorCarga->carga);

    *(nodoConMenorCarga->carga) = carga +1;

    printf("    Nodo %d, Bloque %d\n",*(newMap -> idNodo),*(newMap -> idBloque));

    list_replace(nodosList, i, nodoConMenorCarga);

    return pos;
}

/*
 * Elige un nodo para cada elemento de la lista nodo_bloque
 * siguiendo el criterio del que menos carga tenga
 *
 */
void asignarNodos(char* archivos){

	printf("Asignando maps: \n\n");

	int arrayNodosId[3];
	int arrayNumBloque[3];

	char* mensajeAJob = string_new();
	string_append(&mensajeAJob, MAP);
	string_append(&mensajeAJob, "*");

	char** archivosEnNodo = string_split(archivos, "#");

	int w = 0;

	while(archivosEnNodo[w] != NULL){

		char** bloques = string_split(archivosEnNodo[w], "*");
		int cantBloques = atoi(bloques[0]);

		//arrays asociados

		int i;
		for (i = 1; i <= cantBloques; i++){

			//bloques[i] == las 3 ubicaciones de cada bloque del archivo
			char** ubicaciones = string_split(bloques[i], "-");


			int j;
			char** nodo_bloque;
			for (j = 0; j < 3; j++){
				nodo_bloque = string_split(ubicaciones[j], "_");
				int nodo = atoi(nodo_bloque[0]);
				int bloque = atoi(nodo_bloque[1]);
				arrayNodosId[j] = nodo;
				arrayNumBloque[j] = bloque;
			}

			//en este array tengo los id de los 3 nodos con el mismo bloque de archivo
			//de esos 3, elige el que menor carga tenga y le asigna el map
			int pos = asignarANodoConMenorTrabajo(arrayNodosId, arrayNumBloque);

		    bool _by_id(t_nodo* n1) {
		        return *(n1->id) == arrayNodosId[pos];
		    }

		    t_nodo* n = list_find(nodosList, (void*)_by_id);

			char* resultFileName = string_duplicate(TEMPLATE);
		    string_append(&resultFileName, string_itoa(*(n->id)));
//		    printf("RESULTFILENAME %s%s\n", resultFileName, string_itoa(arrayNumBloque[pos]));

			string_append(&mensajeAJob, n->ip);
			string_append(&mensajeAJob, ",");
			string_append(&mensajeAJob, n->puerto);
			string_append(&mensajeAJob, ",");
			string_append(&mensajeAJob, string_itoa(*(n->id)));
			string_append(&mensajeAJob, ",");
			string_append(&mensajeAJob, string_itoa(arrayNumBloque[pos]));
			string_append(&mensajeAJob, ",");
			string_append(&mensajeAJob, string_duplicate(resultFileName));
			string_append(&mensajeAJob, string_itoa(arrayNumBloque[pos]));
			string_append(&mensajeAJob, "*");

		}

		w++;
	}


	//una falta de respeto al buen codigo, es un mensaje de mas de 1000b y nos caga el puto protocolo
	//y bueno..

	if(strlen(mensajeAJob) > 999){
		char* nuevoheader = string_new();
		string_append(&nuevoheader, SOY_MARTA); //1 digito
		string_append(&nuevoheader, string_itoa(strlen(mensajeAJob)));//4 digitos

//		printf("NUEVO HEADER ASIGNAR NODOS : %s \n", nuevoheader);

		enviar(auxGlobalSocket, "9999", 4); //falso header
		enviar(auxGlobalSocket, nuevoheader, 5); //header de 5
	}else{
		sendHeader(SOY_MARTA, strlen(mensajeAJob), auxGlobalSocket);
//		printf("LENGTH HEADER ASIGNAR NODOS : %d \n", strlen(mensajeAJob));
	}

	enviar(auxGlobalSocket, mensajeAJob, strlen(mensajeAJob));

	printf("\nMaps enviados a job.\n");

}

char* pedirArchivosAlFs(char* listaArchivos) {
//	 char* listaArchivos = string_substring_from(buffer, 1);
	 char** arrayRutasArchivos = string_split(listaArchivos, "*");
	 char* mensajeRecibido = string_new();

	 int x = 0;
	 while (arrayRutasArchivos[x] != '\0')
	 {
	  char* mensaje = string_new();
	  //string_append(&mensaje, SOY_MARTA);
	  string_append(&mensaje, PIDO_UBICACION);
	  string_append(&mensaje, arrayRutasArchivos[x]);

//	  printf("mensaje %s\n", mensaje);

	  sendHeader(SOY_MARTA, strlen(mensaje), socketFS);
	  enviar(socketFS, mensaje, strlen(mensaje));

	  char buf[1024];

	  recibir(socketFS, buf, 1024);

	  string_append(&mensajeRecibido, string_substring(buf, 0, string_length(buf))); //onda realloc
	  string_append(&mensajeRecibido, "#");
	  x++;
	 }

	 //EJEMPLO DE UN SOLO ARCHIVO
	 //cant bloques  bloque1ubicacion1 - bloque1ubicacion2 - bloque1ubicacion3  bloque2ubicacion1 - bloque2ubicacion2 - bloque2ubicacion3

	 //printf("Ubicacion recibida: %s", mensajeRecibido);

	 return mensajeRecibido; //ejemplo de respuesta de FS

}

void conectarAFS(){

	char* mensaje = string_new();

	string_append(&mensaje, "0");

	int length = strlen(mensaje);

	socketFS = conectar(IP_FS, PUERTO_FS);
	sendHeader(SOY_MARTA, length, socketFS);
	enviar(socketFS, mensaje, length);

}

void mocks(){

	t_nodo* n = malloc(sizeof(t_nodo));
	n->carga = malloc(sizeof(int));
	n->id = malloc(sizeof(int));
	n->ip = string_new();
	n->puerto = string_new();

	*(n->carga) = 0;
	*(n->id) = 1;
	string_append(&(n->ip), "127.0.0.1");
	string_append(&(n->puerto), "6000");

	list_add(nodosList, n);

}

//EL FACIL
void planificacionConCombiner(t_job* job){

	//reduces locales
	char* resultFileName = string_new();
	string_append(&resultFileName, TEMPLATEREDUCE);


    bool _by_idNodo(t_map* m1, t_map* m2) {
        return *(m1->idNodo) <= *(m2->idNodo);
    }

	list_sort(job->maps, (void*) _by_idNodo);

	char* mensajeAJob = string_new();
	string_append(&mensajeAJob, REDUCE);
	string_append(&mensajeAJob, "*");

	int i;
	int c = job->maps->elements_count;

	int idActual = -1; //valor inicial

	char* nodoActual;

//	t_reduce* r;

	for (i = 0; i < c; i++){ //foreach map

		t_reduce* r;

		t_map* m = list_get(job->maps, i);

		if(*(m->idNodo) != idActual){

			if(idActual != -1) {

				list_add(job->reduces, r);

				string_append(&mensajeAJob, "_");
			}

			r = malloc(sizeof(t_reduce));
			r->nombreArchivo = string_new();
			r->cantArchivos=0;
			r->idNodo=0;
			r->isCopyFinal=0;
			r->terminado=0;

			idActual = *(m->idNodo);
			bool _eq_idNodo(t_nodo* n) {
				return *(n->id) == *(m->idNodo);
			}

			t_nodo* n = list_find(nodosList, (void*)_eq_idNodo);

			string_append(&mensajeAJob, string_itoa(*(n->id)));
			string_append(&mensajeAJob, ",");
			string_append(&mensajeAJob, n->ip);
			string_append(&mensajeAJob, ",");
			string_append(&mensajeAJob, n->puerto);
			string_append(&mensajeAJob, ",");


			r->idNodo = *(n->id);
			r->terminado = 0;

			nodoActual = n->ip;

		}

		string_append(&mensajeAJob, string_duplicate(resultFileName));
		string_append(&mensajeAJob, string_itoa(*(m->idNodo)));
		string_append(&mensajeAJob, string_itoa(*(m->idBloque)));
		string_append(&mensajeAJob, "-");

		r->cantArchivos++;

		if(i == (c-1)) list_add(job->reduces, r); //cambiar que es un asco, solo para test, SOLO TEST (pero anda eh, anda bien)
		//manotazo de ahogado

	}

	//saco el ultimo -
	mensajeAJob = string_substring_until(mensajeAJob, strlen(mensajeAJob)-1);

	puts("Enviando mensaje reduce local a job");
	//mensaje: REDUCE*ip,puerto,arch1-arch2-arch3-ip,puerto,arch1-arch2-...


	if(strlen(mensajeAJob) > 999){
		char* nuevoheader = string_new();
		string_append(&nuevoheader, SOY_MARTA); //1 digito
		string_append(&nuevoheader, string_itoa(strlen(mensajeAJob)));//4 digitos

//		printf("NUEVO HEADER PLANIF COMBINER : %s \n", nuevoheader);

		enviar(*(job->socket), "9999", 4); //falso header
		enviar(*(job->socket), nuevoheader, 5); //header de 5
	}else{
		sendHeader(SOY_MARTA, strlen(mensajeAJob), *(job->socket));
//		printf("LENGTH HEADER PLANIF COMBINER : %d \n", strlen(mensajeAJob));
	}
	enviar(*(job->socket), mensajeAJob, strlen(mensajeAJob)); //reduce local

}



//EL DIFICIL
void planificacionSinCombiner(t_job* job){

	//Primero le vamos a mandar el mensaje al job para que copie
	//los archivos al nodo que mas tenga, y despues le mandamos otro mensaje
	//para que haga el reduce en este nodo

	bool _by_idNodo(t_map* m1, t_map* m2) {
		return *(m1->idNodo) <= *(m2->idNodo);
	}
	list_sort(job->maps, (void*) _by_idNodo);

	int i;
	int c = job->maps->elements_count;
	int idActual = -1; //valor inicial

	int auxContadorMasArchivos = 0;
	int auxContadorMasArchivos2 = 0;
	char* auxNodoConMasArchivosID = string_new();
	char* auxNodoConMasArchivosIP = string_new();
	char* auxNodoConMasArchivosPuerto = string_new();
	char* NodoConMasArchivosID = string_new();
	char* NodoConMasArchivosIP = string_new();
	char* NodoConMasArchivosPuerto = string_new();
	char* nodoActual = string_new();

	//checkeamo que haga falta copiar archivos (que no esten todos en el mismo nodo)

	t_map* xx22 = list_get(job->maps, 0);

	bool _distinto_nodo (t_map* m){
		return *(m->idNodo) != *(xx22->idNodo);
	}

	if (list_any_satisfy(job->maps, (void*) _distinto_nodo)) { //if deberiaCopiarEnNodoPrincipal
		//aca saca que nodo tiene mas archivos
		for (i = 0; i < c; i++){ //foreach map

			t_map* m = list_get(job->maps, i);

			if(*(m->idNodo) != idActual){

				auxContadorMasArchivos = 0;

				idActual = *(m->idNodo);
				bool _eq_idNodo(t_nodo* n) {
					return *(n->id) == *(m->idNodo);
				}

				t_nodo* n = list_find(nodosList, (void*)_eq_idNodo);

				//si es el primero nomas
				if(string_equals_ignore_case(auxNodoConMasArchivosIP, ""))
				{
					auxNodoConMasArchivosID = string_itoa(*(n->id));
					auxNodoConMasArchivosIP = n->ip;
					auxNodoConMasArchivosPuerto = n->puerto;
				}

				nodoActual = string_itoa(*(n->id));
			}

			auxContadorMasArchivos++;
			if(auxContadorMasArchivos > auxContadorMasArchivos2)
			{
				NodoConMasArchivosID = auxNodoConMasArchivosID;
				NodoConMasArchivosIP = auxNodoConMasArchivosIP;
				NodoConMasArchivosPuerto = auxNodoConMasArchivosPuerto;
				auxContadorMasArchivos2 = auxContadorMasArchivos;
			}
		}


		//Mensaje para copiar archivos
		char* mensajeAJobCopiar = string_new();
		string_append(&mensajeAJobCopiar, COPIAR_ARCHIVOS);
		string_append(&mensajeAJobCopiar, "*");
		string_append(&mensajeAJobCopiar, "*");

		c = job->maps->elements_count;

		idActual = -1; //vuelve a inicializar

		for (i = 0; i < c; i++){ //foreach map

			t_map* m = list_get(job->maps, i);

			if(*(m->idNodo) == atoi(NodoConMasArchivosID)) continue; //para que no agregue los archivos del nodo al que le va a mandar el mensaje

			if(*(m->idNodo) != idActual){

				idActual = *(m->idNodo);
				bool _eq_idNodo(t_nodo* n) {
					return *(n->id) == *(m->idNodo);
				}

				t_nodo* n = list_find(nodosList, (void*)_eq_idNodo);

				mensajeAJobCopiar = string_substring_until(mensajeAJobCopiar, strlen(mensajeAJobCopiar)-1);
				string_append(&mensajeAJobCopiar, "#");

				string_append(&mensajeAJobCopiar, n->ip);
				string_append(&mensajeAJobCopiar, ",");
				string_append(&mensajeAJobCopiar, n->puerto);
				string_append(&mensajeAJobCopiar, ",");

				nodoActual = string_itoa(*(n->id));
			}

			string_append(&mensajeAJobCopiar, TEMPLATE);
			string_append(&mensajeAJobCopiar, string_itoa(*(m->idNodo)));
			string_append(&mensajeAJobCopiar, string_itoa(*(m->idBloque)));
			string_append(&mensajeAJobCopiar, "-");

			auxContadorMasArchivos++;
			if(auxContadorMasArchivos > auxContadorMasArchivos2)
			{
				NodoConMasArchivosIP = auxNodoConMasArchivosIP;
				NodoConMasArchivosPuerto = auxNodoConMasArchivosPuerto;
				auxContadorMasArchivos2 = auxContadorMasArchivos;
			}
		}

		//:COPIAR_ARCHIVOS*ip, puerto,arch1-arch2-ip, puerto,arch4-arch5_nodoDondeCopiar, puerto
		string_append(&mensajeAJobCopiar, "_");
		string_append(&mensajeAJobCopiar, NodoConMasArchivosIP);
		string_append(&mensajeAJobCopiar, ",");
		string_append(&mensajeAJobCopiar, NodoConMasArchivosPuerto);

		sendHeader(SOY_MARTA, strlen(mensajeAJobCopiar), *(job->socket));
		enviar(*(job->socket), mensajeAJobCopiar, strlen(mensajeAJobCopiar));
	}
	else
	{

		bool _by_id_nodo(t_nodo* n){
			return *(n->id) == *(xx22->idNodo);
		}

		t_nodo* n = list_find(nodosList, (void*) _by_id_nodo);

		char* listArchivos = string_new();

		int i=0;
		while(list_size(job->maps) > i)
		{
			t_map* map = list_get(job->maps, i);
			char* nombreArchivo = string_new();
			string_append(&nombreArchivo, "/mapb");
			string_append(&nombreArchivo, string_itoa(*(map->idNodo)));
			string_append(&nombreArchivo, string_itoa(*(map->idBloque)));
			string_append(&listArchivos, nombreArchivo);
			string_append(&listArchivos, "-");

			i++;
		}

		char* mensajeAJob = string_new();
		string_append(&mensajeAJob, REDUCE_FINAL);
		string_append(&mensajeAJob, "*");

		string_append(&mensajeAJob, n->ip);
		string_append(&mensajeAJob, ",");
		string_append(&mensajeAJob, n->puerto);
		string_append(&mensajeAJob, ",");
		string_append(&mensajeAJob, listArchivos);

		sendHeader(SOY_MARTA, strlen(mensajeAJob), *(job->socket));
		enviar(*(job->socket), mensajeAJob, strlen(mensajeAJob));
	}

}


void planificarReduces(t_job* job){

	if(*(job->combiner)){
		planificacionConCombiner(job);
	}
	else{
		planificacionSinCombiner(job);
	}

}

void switchFS(int socket, char* buffer){

	char* mensaje = string_new();
	string_append(&mensaje, string_substring_from(buffer, 1));

	switch(buffer[0]){

		case NUEVONODO:{

			agregarNodo(mensaje);

			break;
		}

		case DESCONEXIONNODO: {

			sacarNodo(mensaje);

			break;
		}

	}

}


void switchJob(int socket, char* buffer){

	char* mensaje = string_new();
	string_append(&mensaje, string_substring_from(buffer, 1));


	switch(buffer[0]){

		case CONNECT:{
//			printf("/n[DEBUG]recibido CONNECT: %s\n", mensaje);

			char* mensaje2 = string_new();
			string_append(&mensaje2, string_substring_from(mensaje, 1));

			char** mensajeArray = string_split(mensaje2, ",");

			char* listaArchivos = mensajeArray[0];
			char* rutaResultado = mensajeArray[1];

			auxGlobalSocket = socket;

			//agregar socket a estructura jobs
			if(!jobConectado()){
				int combiner = buffer[1] - '0'; //hack (convierte char a int)
				agregarJob(combiner, rutaResultado);
			}

			char* archivosFS = string_new();
			string_append(&archivosFS, pedirArchivosAlFs(listaArchivos));

			if(atoi(archivosFS) == -1){
				printf("El FS se encuentra en estado NO OPERATIVO o no existe el archivo/directorio. \n");
				printf("Abortando job.\n");

				char* mensajeJob = string_new();
				string_append(&mensajeJob, ABORTARJOB);

		    	sendHeader(SOY_MARTA, strlen(mensajeJob), socket);
		    	enviar(socket, mensajeJob, strlen(mensajeJob));

				break;
			}

			asignarNodos(archivosFS); //La planificacion de maps es igual tenga o no combiner

			break;
		}

		case ENDCOPYFILES_SIN_COMBINER:{
//			printf("[DEBUG]recibido ENDCOPPYFILES_SIN_COMBINER: %s\n", mensaje);

			//Aca vamos a hacer el reduce final en el nodo que nos pase el job

			bool _by_id(t_job* j) {
				return *(j->socket) == socket;
			}

			//busco el job que me dice que termino de copiar los archivos
			t_job* job = list_find(jobsList, (void*)_by_id);

			char** arrayDatosNodo = string_split(mensaje, ",");

			char* ipNodo = arrayDatosNodo[0];
			char* puertoNodo = arrayDatosNodo[1];

			//int listaArchivos = atoi(arrayDatosNodo[2]);
			char* listArchivos = string_new();

			int i=0;
			while(list_size(job->maps) > i)
			{
				t_map* map = list_get(job->maps, i);
				char* nombreArchivo = string_new();
				string_append(&nombreArchivo, "/mapb");
				string_append(&nombreArchivo, string_itoa(*(map->idNodo)));
				string_append(&nombreArchivo, string_itoa(*(map->idBloque)));
				string_append(&listArchivos, nombreArchivo);
				string_append(&listArchivos, "-");

				i++;
			}

			//int resultado = atoi(arrayDatosNodo[3]);

			//if (resultado == 0){
				char* mensajeAJob = string_new();
				string_append(&mensajeAJob, REDUCE_FINAL);
				string_append(&mensajeAJob, "*");

				string_append(&mensajeAJob, ipNodo);
				string_append(&mensajeAJob, ",");
				string_append(&mensajeAJob, puertoNodo);
				string_append(&mensajeAJob, ",");
				string_append(&mensajeAJob, listArchivos);

//				printf("LENGTH MENSAJE A JOB ENDCOPYFILES: %d\n", strlen(mensajeAJob));

				sendHeader(SOY_MARTA, strlen(mensajeAJob), *(job->socket));
				enviar(*(job->socket), mensajeAJob, strlen(mensajeAJob));

			break;
		}

		case ENDMAP:{

//			printf("[DEBUG]recibido ENDMAP: %s\n", mensaje);

		    bool _by_id(t_job* j) {
		        return *(j->socket) == socket;
		    }

		    //busco el job que me dice que termino un map
		    t_job* job = list_find(jobsList, (void*)_by_id);

			char** arrayDatosMap = string_split(mensaje, ",");

			int idNodo = atoi(arrayDatosMap[0]);
			int idBloque = atoi(arrayDatosMap[1]);

			int resultado;
			if(arrayDatosMap[2] != NULL)
				resultado = ((arrayDatosMap[2])[0]) - '0'; //no se entiende una mierda, pero es por si viene el buffer sucio (solo pueden ser 0 o 1)
			else
				resultado = -1; //si vino en null se cago algo, replanifica

			//si el map termino bien
			if (resultado == 0){

				printf("El map al nodo %d, bloque %d termino bien \n", idNodo, idBloque);

				bool _by_id_and_bloque(t_map* m){
					return (*(m->idNodo) == idNodo && *(m->idBloque) == idBloque);
				};

				//pongo el map en estado terminado
				t_map* map = list_remove_by_condition(job->maps, (void*) _by_id_and_bloque);

				*(map->terminado) = 1;

				list_add(job->maps, map);

				//////////////
				//agrego para que imprima el progreso
				/////////////

				int terminados = 0;
				int totales = job->maps->elements_count;

				void _count(t_map* m){
					if( *(m->terminado)) terminados++;
				}

				list_iterate(job->maps, (void*)_count);

				printf("%d / %d maps del job en socket %d terminados.\n", terminados, totales, *(job->socket));
			}
			else{//si el map fallo
				printf("El map al nodo %d, bloque %d fallo \n", idNodo, idBloque);
				replanificarJobNuevoNodo(job, idNodo, idBloque);
			}

			bool _nodo_by_id(t_nodo* n){
				return (*(n->id) == idNodo);
			}

			//le bajo un nivel de carga al nodo (falle o no falle)

			t_nodo* n = list_remove_by_condition(nodosList, (void*)_nodo_by_id);

			if(n != NULL){ // si el nodo sigue conectado
				*(n->carga) = *(n->carga) - 1;
				list_add(nodosList, n);
			}

			//////////////////////////////////////////////////////////////////////////////////////////////////

//		    bool _maps_terminados(t_job* j){
//		    	//doble closure yeah
		    	bool _map_terminado(t_map* m){
		    		return (  *(m->terminado) == 1);
		    	}
//
//		    	return list_all_satisfy(j->maps, (void*)_map_terminado);
//		    }

		    	//single closure
		    //SI TODOS LOS OTROS MAPS DEL JOB QUE ACABA DE TERMINAR SU MAP ESTAN LISTOS
		    int termino = list_all_satisfy(job->maps, (void*) _map_terminado);

		    if(termino){
				planificarReduces(job);
		    }

			break;
		}


		case ENDCOPYFILES:{

//			printf("[DEBUG]recibido ENDCOPYFILES: %s\n", mensaje);

			 bool _by_id(t_job* j) {
				return *(j->socket) == socket;
			}

			//busco el job que me dice que termino de copiar
			t_job* job = list_find(jobsList, (void*)_by_id);


			bool _by_id_reduces (t_reduce* r){
				return r->isCopyFinal == 1;
			}

			t_reduce* r = list_find(job->reduces, (void*)_by_id_reduces);

			bool _by_id_nodo (t_nodo* n){
				return *(n->id) == r->idNodo;
			}

			t_nodo* n = list_find(nodosList, (void*)_by_id_nodo);

			char* mensajeAJobReduceFinal = string_new();
			string_append(&mensajeAJobReduceFinal, REDUCE_FINAL);
			string_append(&mensajeAJobReduceFinal, "*");
			string_append(&mensajeAJobReduceFinal, n->ip);
			string_append(&mensajeAJobReduceFinal, ",");
			string_append(&mensajeAJobReduceFinal, n->puerto);
			string_append(&mensajeAJobReduceFinal, ",");

			int x;//
			for(x = 0 ; x < list_size(job->reduces); x++){

				t_reduce* red = list_get(job->reduces, x);

				string_append(&mensajeAJobReduceFinal, "/");
				string_append(&mensajeAJobReduceFinal, red->nombreArchivo);
				string_append(&mensajeAJobReduceFinal, "-");
			}

//			char* nuevoheader = string_new();
//			string_append(&nuevoheader, SOY_MARTA); //1 digito
//			string_append_with_format(&nuevoheader, "%d", strlen(mensajeAJobReduceFinal));//4 digitos
//
//			enviar(*(job->socket), "9999", 4); //falso header
//			enviar(*(job->socket), nuevoheader, 5); //header de 5
			sendHeader(SOY_MARTA, strlen(mensajeAJobReduceFinal), *(job->socket));
			enviar(*(job->socket), mensajeAJobReduceFinal, strlen(mensajeAJobReduceFinal));


			break;
		}

		case ENDREDUCE:{

//			printf("[DEBUG]recibido ENDREDUCE: %s\n", mensaje);

			bool _by_id(t_job* j) {
				return *(j->socket) == socket;
			}

			//busco el job que me dice que termino un reduce
			t_job* job = list_find(jobsList, (void*)_by_id);

			char** arrayDatosReduce = string_split(mensaje, ",");

			int local, resultado, id;
			char* nombreArchivo = string_new();

			//SI EL NODO FALLA, DEVUELVE ",local" (local == 1 || local == 0)
			if(arrayDatosReduce[1] == NULL){
				local = atoi(arrayDatosReduce[0]);
				resultado = -1;

			}else{
				id = atoi(arrayDatosReduce[0]);
				resultado = atoi(arrayDatosReduce[1]);
				string_append(&nombreArchivo, arrayDatosReduce[2]);
				local = atoi(arrayDatosReduce[3]);
			}

			if (resultado != 0)
			{
				printf("El reduce en el nodo %d fallo \n", id);
				printf("Abortando job.\n");

				char* mensajeJob = string_new();
				string_append(&mensajeJob, ABORTARJOB);

		    	sendHeader(SOY_MARTA, strlen(mensajeJob), socket);
		    	enviar(socket, mensajeJob, strlen(mensajeJob));

		    	list_remove_by_condition(jobsList, (void*)_by_id); //lo volamo si aborto

			}
			else{
				printf("El reduce en el nodo %d termino bien \n", id);
				if (!local){
					char* mensajeAJob = string_new();
						string_append(&mensajeAJob, TERMINADO);

					sendHeader(SOY_MARTA, strlen(mensajeAJob), socket);
					enviar(socket, mensajeAJob, strlen(mensajeAJob));

					list_remove_by_condition(jobsList, (void*)_by_id); //lo volamo si termino sin combiner
					//TODO: liberar la memoria antes

					//soy_marta 2nombreArchivo,id,rutaresultado
					char* mensajeAFS = string_new();
					string_append(&mensajeAFS,"2");
					string_append(&mensajeAFS,nombreArchivo);
					string_append(&mensajeAFS,",");
					string_append(&mensajeAFS,string_itoa(id));
					string_append(&mensajeAFS,",");
					string_append(&mensajeAFS, job->rutaResultado);

					sendHeader(SOY_MARTA,strlen(mensajeAFS),socketFS);
					enviar(socketFS,mensajeAFS,strlen(mensajeAFS));
				}
				else
				{
					bool _by_id (t_reduce* r){
						return r->idNodo == id;
					}

					t_reduce* r = list_find(job->reduces, (void*)_by_id);

					string_append(&(r->nombreArchivo), nombreArchivo);
					r->terminado = 1;


//				    bool _reduces_terminados(t_job* j){
//				    	//doble closure yeah
				    	bool _reduce_terminado(t_reduce* r){
				    		return ( r->terminado == 1 );
				    	}
//
//				    	return list_all_satisfy(j->reduces, (void*)_reduce_terminado);
//				    }
				    	//single :(

				    //SI TODOS LOS OTROS REDUCES DEL JOB QUE ACABA DE TERMINAR SU REDUCE ESTAN LISTOS
				    int termino = list_all_satisfy(job->reduces, (void*) _reduce_terminado);
//
				    if(termino){

				    	char* mensajeAJobCopiar = string_new();
				    	string_append(&mensajeAJobCopiar, COPIAR_ARCHIVOS);
				    	string_append(&mensajeAJobCopiar, "*");

				    	//al ser local, hay un archivo reducido por nodo, le manda a hacer el reduce final al 1ro de la lista porque si
				    	t_reduce* reduceFinal = list_get(job->reduces, 0);
						reduceFinal->isCopyFinal = 1;


						bool _by_id_reduces (t_nodo* nodo){
							return *(nodo->id) == reduceFinal->idNodo;
						}
				    	t_nodo* nodoDondeCopiar = list_find(nodosList, (void*)_by_id_reduces);


				    	int x;
				    	for(x=0; x < list_size(job->reduces); x++)
				    	{
				    		t_reduce* reduce = list_get(job->reduces, x);

							bool _by_id(t_nodo* n) {
								return *(n->id) == reduce->idNodo;
							}

							t_nodo* nodoCopiar = list_find(nodosList, (void*)_by_id);

							if(nodoDondeCopiar->id != nodoCopiar->id)
							{
								string_append(&mensajeAJobCopiar, nodoCopiar->ip);
								string_append(&mensajeAJobCopiar, ",");
								string_append(&mensajeAJobCopiar, nodoCopiar->puerto);
								string_append(&mensajeAJobCopiar, ",");
								string_append(&mensajeAJobCopiar, reduce->nombreArchivo);
								string_append(&mensajeAJobCopiar, "#");
							}
				    	}

				    	//saco el ultimo #
				    	mensajeAJobCopiar = string_substring_until(mensajeAJobCopiar, strlen(mensajeAJobCopiar)-1);

						string_append(&mensajeAJobCopiar, "_");
				    	string_append(&mensajeAJobCopiar, nodoDondeCopiar->ip);
				    	string_append(&mensajeAJobCopiar, ",");
				    	string_append(&mensajeAJobCopiar, nodoDondeCopiar->puerto);

				    	puts("Enviando mensaje copiar archivos a nodo");
				    	sendHeader(SOY_MARTA, strlen(mensajeAJobCopiar), *(job->socket));
				    	enviar(*(job->socket), mensajeAJobCopiar, strlen(mensajeAJobCopiar));
				    }

				}
			}

			break;
		}

		default: {
			printf("\n Mensaje de job desconocido \n");
		}

	}

}


int main(void) {

	jobsList = list_create();
	nodosList = list_create();

	char* propiedades[3] = {"PUERTO_FS", "IP_FS", "PUERTO_LISTEN"};
	char** variables[3] = {&PUERTO_FS, &IP_FS, &PUERTO_LISTEN};

	inicializarStrings(variables, 3);

	leerConfig(CONFIG_PATH, propiedades, variables, 3);

	int listener = crearListener(atoi(PUERTO_LISTEN));
	t_struct_select* parametros = inicializarSelect(listener, 1024);

	conectarAFS();

	FD_SET(socketFS, &parametros->master);
	parametros->temp = parametros->master;
	parametros->maxSock ++;

	while (1) {

		int socket = getSocketChanged(parametros);

		if(socket == -1)
			continue;

		char* buffer = string_new();
		string_append(&buffer, parametros->buffer);

//		printf("[DEBUG] BUFFER RECIBIDO: %s\n", buffer);

		if(parametros->owner == JOB)
			switchJob(socket, buffer);
		if(parametros->owner == FILESYSTEM)
			switchFS(socket, buffer);

		free(parametros->buffer);
	}

	return EXIT_SUCCESS;
}
