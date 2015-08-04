/*
 ============================================================================
 Name        : Filesystem.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */
#define _GNU_SOURCE

#include "Filesystem.h"
#include "Libs.h"
#include <bson.h>
#include <mongoc.h>
#include <string.h>




//ver como usar la ruta relativa
#define CONFIG_PATH "/home/utnso/tp-2015-1c-souvenir-jackitos/Filesystem/src/config.cfg"

int socketMarta = -1;
int PUERTO_LISTEN;
int LISTA_NODOS;
int estado;
mongoc_client_t *client;
mongoc_collection_t *collectionArchivo;
mongoc_collection_t *collectionNodoListOn;
mongoc_collection_t *collectionNodoListOff;
mongoc_collection_t *collectionGlobales;
mongoc_collection_t *collectionDirectorio;

t_log* loggerMDFS;

void switchMarta(int socket, char* buffer) {

	switch (buffer[0]) {
	case '0': { //conecta, setea variable socketMarta
		socketMarta = socket;
		printf("\n MaRTA se ha conectado al filesystem.\n");
//			log_info(loggerMDFS, "MaRTA se ha conectado al filesystem.");

		int i;
		for (i = 0; i < list_size(nodosListOn); i++) {
			t_nodo* nodo = list_get(nodosListOn, i);
			char* mensaje = string_new();
			string_append(&mensaje, "1"); //'1' : enviando nodo conectado a MaRTA
			char* nodo_id_char = string_new();
			nodo_id_char = string_itoa(nodo->id);
			string_append(&mensaje, nodo_id_char);
			string_append(&mensaje, ",");
			string_append(&mensaje, nodo->ip);
			string_append(&mensaje, ",");
			string_append(&mensaje, nodo->puerto);

			int strlenMensaje = strlen(mensaje);

			sendHeader(SOY_FS, strlenMensaje, socketMarta);
			enviar(socketMarta, mensaje, strlen(mensaje));
			log_info(loggerMDFS,
					"Se comunico a MaRTA la conexion del nodo : %d", nodo->id);
		}

		break;
	}

	case '1': { //pide ubicacion
		//buffer[2] = ruta del archivo en mdfs
		char* ruta = string_substring_from(buffer, 1);

		// PROTOCOLO: cantBloques*nodocopia1_bloque1-nodocopia2_bloque1-nodocopia3_bloque1*nodocopia1_bloque2-nodocopia2_bloque2-nodocopia3_bloque2
		//nodocopiax es el id_nodo

		if (estado == OPERATIVO) {
			enviarUbicacionMaRTA(ruta);
		} else {
			printf("ESTADO NO OPERATIVO: para el File %s\n", ruta);
			enviar(socketMarta, "-1", 3);
		}

		break;
	}

	case '2': { //pide grabar resultado
		//2nombre,id,ruta_completa
		char* mensaje_buffer = string_substring_from(buffer, 1);
		char** array = string_split(mensaje_buffer, ",");
		char* name = string_new();
		char* ruta_completa = string_new();
		int id_nodo;

		name = array[0];
		id_nodo = atoi(array[1]);
		ruta_completa = array[2];

		t_nodo* nodo;
		bool _obtener_nodo_por_id(t_nodo* n1) {
			return n1->id == id_nodo;
		}

		nodo = list_find(nodosListOn, (void*) _obtener_nodo_por_id);
		if (nodo == NULL) {
			printf("\nEl nodo id: %d no se encuentra conectado.", id_nodo);
			return;
		}

		//	valido que exista /user/asd/algo

		char** split = string_split(ruta_completa, "/");
		int tamanioRuta = tamanioArray(split);
		int t;
		char* rutaSinArchivo = string_new();
		if (tamanioRuta == 1) {

			string_append(&rutaSinArchivo, "/");

		} else {
			for (t = 0; t < (tamanioRuta - 1); t++) {

				string_append(&rutaSinArchivo, "/");
				string_append(&rutaSinArchivo, split[t]);

			}
		}

		t_list* caminoDirectorio = armarDir(rutaSinArchivo);
		if (list_is_empty(caminoDirectorio)) {
			error_show("ERROR: El directorio del resultado no existe. \n");
			log_info(loggerMDFS,
					"ERROR: El directorio del resultado no existe. ");
			enviar(socketMarta, "-1", 3);
			return;
		}

		char* buffer = string_new();
		string_append(&buffer, GET_RESULTADO); //'2' GET_RESULTADO
		string_append(&buffer, name);
		int strlenBuffer = strlen(buffer);
		sendHeader(SOY_FS, strlenBuffer, nodo->id_socket);
		enviar(nodo->id_socket, buffer, strlenBuffer);

		char bufferHeader[HEADERSIZE];
		recibir(nodo->id_socket, bufferHeader, HEADERSIZE);

//		printf("[DEBUG] header recibido: %s\n", bufferHeader);

		char* msj1 = string_new();
		msj1 = string_substring_from(bufferHeader, 1);

		char* mensaje = malloc(atoi(msj1));
		recibir(nodo->id_socket, mensaje, atoi(msj1));

//		printf("[DEBUG] mensaje con length recibido: %s\n", mensaje);

		int data_resultado_size = atoi(mensaje);

		void* data_resultado = malloc(data_resultado_size);
		int n;
		int s = 0;
		while ((s != data_resultado_size)
				&& ((n = recv(nodo->id_socket, data_resultado + s,
						data_resultado_size - s, 0)) > 0)) {
			s = s + n;
//			printf("[DEBUG] bytes recibidos: %d / %d \n", s,
//					data_resultado_size);
		}

		printf("archivo recibido\n");

		log_info(loggerMDFS, "Recibo resultado: ");
		log_info(loggerMDFS, "Bytes recibidos: %d / %d \n", s,
				data_resultado_size);

		int cant_bloques;

		int size = list_size(caminoDirectorio);
		t_elemento* dir = list_get(caminoDirectorio, size - 1);

		t_archivo* new_archivo = (t_archivo*) malloc(sizeof(t_archivo));
		char* new_name = string_new();
//			//esta funcion tambien sirve para paths a archivos.
		new_name = obtenerNombreUltimoDirectorio(ruta_completa);
		new_archivo->nombre = new_name;
		new_archivo->l_bloques = list_create();
		new_archivo->size_bytes = data_resultado_size;
		new_archivo->index_directorio_padre = dir->index;

		cant_bloques = getCantidadBloquesArchivo(data_resultado_size);

		if (nodosDisponibles(cant_bloques) == -1) {
			//msj error
			free(data_resultado);
			return;
		}

		int ret;
		int retAcum = 0;

		int j;
		for (j = 0; j < cant_bloques; j++) {

			printf("\nEnviando bloque N° %d ...      ", j);
			log_info(loggerMDFS, "Enviando bloque N° %d ...      ", j);

			ret = enviarDatosANodos((data_resultado + retAcum), new_archivo, j,
					retAcum);
			retAcum += ret;
			printf("Enviados totales %d\n", retAcum);

		}

		log_info(loggerMDFS, "Grabó %d/%d", retAcum, data_resultado_size);

		new_archivo->estado = DISPONIBLE;
		list_add(archivosList, new_archivo);

		insertarEnDBArchivo(new_archivo);

		free(data_resultado);

		break;
	}

	}

}

char* getFileName(char* location){
	char* nombre = malloc(strlen(location + 50));
	realpath(location, nombre);
	printf ("%s\n", nombre);

	char* nombreAlReves = string_reverse(nombre);
	char** arrayNombre = string_n_split(nombreAlReves, 1, "/");
	char* nombrePosta = string_reverse(arrayNombre[0]);

	free(nombreAlReves);
	string_iterate_lines(arrayNombre, (void*) free);
	free(arrayNombre);
	free(nombre);

	return nombrePosta;

}

char* string_reverse(char* palabra) {
	char* resultado = malloc(string_length(palabra) + 1);

	if (string_is_empty(palabra)) {
		return string_new();
	} else {
		int i = string_length(palabra) - 1, j = 0;
		while (i >= 0){
			resultado[j] = palabra[i];
			i--;
			j++;
		}
		resultado[j] = '\0';

		return resultado;
	}
}

void insertarEnDBGlobalesMdfs() {

	bson_error_t error;
	bson_t *mdfs;

	mdfs = bson_new();

	BSON_APPEND_INT32(mdfs, "index_mdfs", index_mdfs);

	if (!mongoc_collection_insert(collectionGlobales, MONGOC_INSERT_NONE, mdfs, NULL, &error)){
			printf("%s\n", error.message);
	}

	bson_destroy(mdfs);

}


bool hayGlobalesMdfs() {

	bool existe = false;
	mongoc_cursor_t *cursor;
	const bson_t *doc;
	bson_t *query;
	bson_t *queryHijo;

	query = bson_new();
	queryHijo = bson_new();

	BSON_APPEND_DOCUMENT_BEGIN(query, "index_mdfs", queryHijo);
	BSON_APPEND_INT32(queryHijo, "$exists", true);
	bson_append_document_end(query, queryHijo);

	cursor = mongoc_collection_find(collectionGlobales, MONGOC_QUERY_NONE, 0, 0,
			0, query, NULL, NULL);

	if (cursor != NULL) {
		while (mongoc_cursor_next(cursor, &doc)) {

			existe = true;

		}
	}

	mongoc_cursor_destroy(cursor);
	bson_destroy(query);
	bson_destroy(queryHijo);

	mongoc_cleanup();

	return existe;

}

void recuperarGlobalesMdfs() {

	mongoc_cursor_t *cursor;
	bson_error_t error;
	const bson_t *doc;
	bson_t *query;
	bson_t *queryHijo;
	bson_iter_t iter;

	query = bson_new();
	queryHijo = bson_new();

	BSON_APPEND_DOCUMENT_BEGIN(query, "index_mdfs", queryHijo);
	BSON_APPEND_INT32(queryHijo, "$exists", true);
	bson_append_document_end(query, queryHijo);

	cursor = mongoc_collection_find(collectionGlobales, MONGOC_QUERY_NONE, 0, 0,
			0, query, NULL, NULL);

	if (cursor != NULL) {
		while (mongoc_cursor_next(cursor, &doc)) {

			bson_iter_init_find(&iter, doc, "index_mdfs");
			index_mdfs = bson_iter_int32(&iter);
		}

		if (mongoc_cursor_error(cursor, &error)) {
			fprintf(stderr, "An error occurred: %s\n", error.message);
		}

	}
	mongoc_cursor_destroy(cursor);
	bson_destroy(query);
	bson_destroy(queryHijo);

	mongoc_cleanup();

}


void updateGloablesMdfs(){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;
	bson_t *queryHijo;

	query = bson_new();
	queryHijo = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	BSON_APPEND_DOCUMENT_BEGIN(query, "index_mdfs", queryHijo);
	BSON_APPEND_INT32(queryHijo, "$exists", true);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set" , updateHijo);
	BSON_APPEND_INT32(updateHijo, "index_mdfs", index_mdfs);
	bson_append_document_end(update, updateHijo);
	bson_append_document_end(query, queryHijo);


	if (!mongoc_collection_update(collectionGlobales, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);
	bson_destroy(queryHijo);

}

void removeDBGlobalesMdfs(){

	bson_error_t error;
	bson_t *query;
	bson_t *queryHijo;

	query = bson_new();
	queryHijo = bson_new();

	BSON_APPEND_DOCUMENT_BEGIN(query, "index_mdfs" , queryHijo);
	BSON_APPEND_INT32(queryHijo, "$exists", true);
	bson_append_document_end(query, queryHijo);

	if(!mongoc_collection_remove(collectionGlobales, MONGOC_REMOVE_SINGLE_REMOVE, query,
			NULL, &error)){
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(queryHijo);

}


void insertarEnDBNodo(t_nodo* nodoNuevo, mongoc_collection_t *collection){

	bson_oid_t oid;
	bson_t *padre;
	bson_t *hijo;
	bson_error_t error;

	memset(&error, '\0', sizeof(bson_error_t));

	padre = bson_new();
	hijo = bson_new();
	bson_oid_init(&oid, NULL);

	BSON_APPEND_INT32(padre, "id", nodoNuevo->id);
	BSON_APPEND_UTF8(padre, "ip", nodoNuevo->ip);
	BSON_APPEND_UTF8(padre, "puerto", nodoNuevo->puerto);
	BSON_APPEND_INT32(padre, "bloques_libres", nodoNuevo->bloques_libres);
	BSON_APPEND_INT32(padre, "size_datos_mb", nodoNuevo->size_datos_mb);
	BSON_APPEND_INT32(padre, "cant_bloques", nodoNuevo->cant_bloques);

	BSON_APPEND_DOCUMENT_BEGIN(padre, "bloques", hijo);

	int x;

	for(x=0; nodoNuevo->cant_bloques>x; x++){

		char* libreX = string_new();
		string_append(&libreX, "libre");
		string_append(&libreX, string_itoa(x));
//		printf ("%i\n", nodoNuevo->bloques[x]);
		BSON_APPEND_INT32(hijo, libreX, nodoNuevo->bloques[x]);
	}

	bson_append_document_end(padre, hijo);

	BSON_APPEND_INT32(padre, "nodo_nuevo", nodoNuevo->nodo_nuevo);

	if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, padre, NULL, &error)){
		printf("%s\n", error.message);
	}

	//destruye los documentos
	bson_destroy(padre);
	bson_destroy(hijo);

}

void updateEstadoBloque(int id, int bloque, int estado, mongoc_collection_t *collection){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

//	int id = nodo->id;

	char* bloqueAUpdetear = string_new();
	string_append(&bloqueAUpdetear, "bloques.libre");
	string_append(&bloqueAUpdetear, string_itoa(bloque));

	BSON_APPEND_INT32(query, "id", id);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set" , updateHijo);
	BSON_APPEND_INT32(updateHijo, bloqueAUpdetear, estado);
	bson_append_document_end(update, updateHijo);

	//update para todos los valores que coinciden con los de la query
	if (!mongoc_collection_update(collection, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}


void updatePuertoNodo(int id, char* puerto, mongoc_collection_t *collection){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

//	int id = nodo->id;

	BSON_APPEND_INT32(query, "id", id);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set" , updateHijo);
	BSON_APPEND_UTF8(updateHijo, "puerto", puerto);
	bson_append_document_end(update, updateHijo);

	//update para todos los valores que coinciden con los de la query
	if (!mongoc_collection_update(collection, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}

void updateIdNodo(int id, int idNuevo, mongoc_collection_t *collection){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

//	int id = nodo->id;

	BSON_APPEND_INT32(query, "id", id);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set" , updateHijo);
	BSON_APPEND_INT32(updateHijo, "id", idNuevo);
	bson_append_document_end(update, updateHijo);

	//update para todos los valores que coinciden con los de la query
	if (!mongoc_collection_update(collectionNodoListOn, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}

void updateIpNodo(int id, char* ip, mongoc_collection_t *collection){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

//	int id = nodo->id;

	BSON_APPEND_INT32(query, "id", id);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set" , updateHijo);
	BSON_APPEND_UTF8(updateHijo, "ip", ip);
	bson_append_document_end(update, updateHijo);

	//update para todos los valores que coinciden con los de la query
	if (!mongoc_collection_update(collection, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}



void updateSocketNodo(int id, int socketNuevo, mongoc_collection_t *collection){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

//	int id = nodo->id;

	BSON_APPEND_INT32(query, "id", id);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set" , updateHijo);
	BSON_APPEND_INT32(updateHijo, "id_socket", socketNuevo);
	bson_append_document_end(update, updateHijo);

	//update para todos los valores que coinciden con los de la query
	if (!mongoc_collection_update(collection, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}

void updateCantidadBloquesLibres(int id, int cantidad, mongoc_collection_t *collection){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

//	int id = nodo->id;

	BSON_APPEND_INT32(query, "id", id);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set" , updateHijo);
	BSON_APPEND_INT32(updateHijo, "bloques_libres", cantidad);
	bson_append_document_end(update, updateHijo);

	//update para todos los valores que coinciden con los de la query
	if (!mongoc_collection_update(collection, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}


void removeNodoDB(int id, mongoc_collection_t *collection){

	bson_error_t error;
	bson_t *remove;

	remove = bson_new();

	BSON_APPEND_INT32(remove, "id", id);

	//remueve el registro cuyo id corresponde al de la query
	if(!mongoc_collection_remove(collection, MONGOC_REMOVE_SINGLE_REMOVE, remove,
			NULL, &error)){
		printf("%s\n", error.message);
	}

	bson_destroy(remove);

}

void recuperarNodoDB(mongoc_collection_t *collection, t_list* lista) {

	mongoc_cursor_t *cursor;
	bson_error_t error;
	const bson_t *doc;
	bson_t *query;
	bson_iter_t iter;
	bson_iter_t iter_hijo;

	query = bson_new();

	cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0,
			query, NULL, NULL);

	if (cursor != NULL) {
		while (mongoc_cursor_next(cursor, &doc)) {

			t_nodo* new_nodo = (t_nodo*) malloc(sizeof(t_nodo));

			bson_iter_init_find(&iter, doc, "id");
			int id = bson_iter_int32(&iter);
			new_nodo->id = id;

			bson_iter_init(&iter, doc);
			bson_iter_init_find(&iter, doc, "ip");
			uint32_t lengthIp;
			char* ip = string_duplicate(bson_iter_utf8(&iter, &lengthIp));
			new_nodo->ip = ip;

			bson_iter_init(&iter, doc);
			bson_iter_init_find(&iter, doc, "puerto");
			uint32_t lengthPuerto;
			char* puerto = string_duplicate(bson_iter_utf8(&iter, &lengthPuerto));
			new_nodo->puerto = puerto;

			bson_iter_init_find(&iter, doc, "size_datos_mb");
			int size_datos_mb = bson_iter_int32(&iter);
			new_nodo->size_datos_mb = size_datos_mb;

//			bson_iter_init_find(&iter, doc, "id_socket");
//			int id_socket = bson_iter_int32(&iter);
			new_nodo->id_socket = -1;

			bson_iter_init_find(&iter, doc, "bloques_libres");
			int bloques_libres = bson_iter_int32(&iter);
			new_nodo->bloques_libres = bloques_libres;

			new_nodo->nodo_nuevo = 0;

			bson_iter_init_find(&iter, doc, "cant_bloques");
			int cant_bloques = bson_iter_int32(&iter);
			new_nodo->cant_bloques = cant_bloques;

			int* array = malloc(sizeof(int) * cant_bloques);

			bson_iter_init_find(&iter, doc, "bloques");
			bson_iter_recurse(&iter, &iter_hijo);

			int x;
			for (x = 0; cant_bloques > x; x++) {

				char* libreX = string_new();
				string_append(&libreX, "libre");
				string_append(&libreX, string_itoa(x));

				bson_iter_find(&iter_hijo, libreX);
				int bloque = bson_iter_int32(&iter_hijo);

				array[x] = bloque;

				bson_iter_recurse(&iter, &iter_hijo);

			}

			new_nodo->bloques = array;

			list_add(nodosListOff, new_nodo);
			removeNodoDB(new_nodo->id, collectionNodoListOff);
			removeNodoDB(new_nodo->id, collectionNodoListOn);
			insertarEnDBNodo(new_nodo, collectionNodoListOff);

		}

		if (mongoc_cursor_error(cursor, &error)) {
			fprintf(stderr, "An error occurred: %s\n", error.message);
		}
	}
	mongoc_cursor_destroy(cursor);
	bson_destroy(query);

}


/*
 * Persistencia archivo
 */


void insertarEnDBArchivo(t_archivo* new_archivo){

	bson_error_t error;
	bson_t *padre;
	bson_t *hijoIntermedio;
	bson_t *hijo;
	bson_t *copiasBson;

	//crea los documentos


	padre = bson_new();
	hijoIntermedio = bson_new();
	hijo = bson_new();
	copiasBson = bson_new();

	//inserta en los documentos
	BSON_APPEND_UTF8(padre, "nombre", new_archivo->nombre);
	BSON_APPEND_INT32(padre, "padre", new_archivo->index_directorio_padre);
	BSON_APPEND_INT32(padre, "tamanio", new_archivo->size_bytes);
	BSON_APPEND_INT32(padre, "estado", new_archivo->estado);

	int cant_bloques;
	cant_bloques = list_size(new_archivo->l_bloques);
	BSON_APPEND_INT32(padre, "cant_bloques", cant_bloques);
//	t_list* bloques = archivo->l_bloques;

	BSON_APPEND_DOCUMENT_BEGIN(padre, "bloques", hijoIntermedio);

	//crea la ubicacion para cada uno de los bloques
	int x;
	for (x = 0; x < cant_bloques; x++) {


		char* ubicacionBloqueX = string_new();
		string_append(&ubicacionBloqueX, "ubicacionBloque");
		string_append(&ubicacionBloqueX, string_itoa(x));
		BSON_APPEND_DOCUMENT_BEGIN(hijoIntermedio, ubicacionBloqueX, hijo);

		//crea la ubicacion x n de cada uno de los bloques
		t_bloques* bloque = list_get(new_archivo->l_bloques, x);
		BSON_APPEND_INT32(hijo, "nroParte", bloque->id_bloque);
		BSON_APPEND_INT32(hijo, "tamanioGrabado", bloque->tamanio_grabado);

		int cant_copias = list_size(bloque->copias);
		BSON_APPEND_INT32(hijo, "cantCopias", cant_copias);
		t_list* copias = bloque->copias;

		int y;
		for (y = 0; y < cant_copias; y++) {

			t_copia* copia = list_get(copias, y);

			char* copiaX = string_new();
			string_append(&copiaX, "copia");
			string_append(&copiaX, string_itoa(y));

			BSON_APPEND_DOCUMENT_BEGIN(hijo, copiaX , copiasBson);
			BSON_APPEND_INT32(copiasBson, "idNodo",copia->id_nodo);
			BSON_APPEND_INT32(copiasBson, "nroBloqueNodo", copia->bloque);
			bson_append_document_end(hijo, copiasBson);

			//free(copia);
		}

		bson_append_document_end(hijoIntermedio, hijo);
		//free(bloque);
	}

	//cierra los documentos padres

	bson_append_document_end(padre, hijoIntermedio);

	if (!mongoc_collection_insert(collectionArchivo, MONGOC_INSERT_NONE, padre, NULL, &error)){
		printf("%s\n", error.message);
	}

	//destruye los documentos
	bson_destroy(copiasBson);
	bson_destroy(hijo);
	bson_destroy(hijoIntermedio);
	bson_destroy(padre);

}


void recuperarArchivos() {

	mongoc_cursor_t *cursor;
	bson_error_t error;
	const bson_t *doc;
	bson_t *query;
	bson_iter_t iter;
	bson_iter_t iterBloques;
	bson_iter_t iterCopias;
	bson_iter_t iterUbicacion;

	query = bson_new();

	cursor = mongoc_collection_find(collectionArchivo, MONGOC_QUERY_NONE, 0, 0,
			0, query, NULL, NULL);

	if (cursor != NULL) {
		while (mongoc_cursor_next(cursor, &doc)) {

			t_archivo* archivo = (t_archivo*) malloc(sizeof(t_archivo));

			bson_iter_init(&iter, doc);
			bson_iter_init_find(&iter, doc, "nombre");
			uint32_t length;
			char* nombre = string_duplicate(bson_iter_utf8(&iter, &length));
			archivo->nombre = nombre;

			bson_iter_init_find(&iter, doc, "padre");
			int padre = bson_iter_int32(&iter);
			archivo->index_directorio_padre = padre;

			bson_iter_init_find(&iter, doc, "tamanio");
			int tamanio = bson_iter_int32(&iter);
			archivo->size_bytes = tamanio;

			bson_iter_init_find(&iter, doc, "estado");
			int estado = bson_iter_int32(&iter);
			archivo->estado = estado;

			bson_iter_init_find(&iter, doc, "cant_bloques");
			int cant_bloques = bson_iter_int32(&iter);

			archivo->l_bloques = list_create();

			bson_iter_init_find(&iter, doc, "bloques");
			bson_iter_recurse(&iter, &iterBloques);

			int x;
			for (x = 0; cant_bloques > x; x++) {

				t_bloques* bloque = (t_bloques*) malloc(sizeof(t_bloques));
				bloque->copias = list_create();

				char* ubicacionBloqueX = string_new();
				string_append(&ubicacionBloqueX, "ubicacionBloque");
				string_append(&ubicacionBloqueX, string_itoa(x));

				bson_iter_find(&iterBloques, ubicacionBloqueX);
				bson_iter_recurse(&iterBloques, &iterUbicacion);

				bson_iter_find(&iterUbicacion, "nroParte");
				int nroBloque = bson_iter_int32(&iterUbicacion);
				bloque->id_bloque = nroBloque;

				bson_iter_find(&iterUbicacion, "tamanioGrabado");
				int tamanioGrabado = bson_iter_int32(&iterUbicacion);
				bloque->tamanio_grabado = tamanioGrabado;

				bson_iter_find(&iterUbicacion, "cantCopias");
				int cantCopias = bson_iter_int32(&iterUbicacion);

				int i;
				for (i = 0; cantCopias > i; i++) {

					t_copia* copia = (t_copia*) malloc(sizeof(t_copia));

					char* copiaX = string_new();
					string_append(&copiaX, "copia");
					string_append(&copiaX, string_itoa(i));

					bson_iter_find(&iterUbicacion, copiaX);
					bson_iter_recurse(&iterUbicacion, &iterCopias);

					bson_iter_find(&iterCopias, "idNodo");
					int idNodo = bson_iter_int32(&iterCopias);
					copia->id_nodo = idNodo;

					bson_iter_find(&iterCopias, "nroBloqueNodo");
					int nroBloque = bson_iter_int32(&iterCopias);
					copia->bloque = nroBloque;

					copia->nodo_online = 0;

					list_add(bloque->copias, copia);

				}

				bson_iter_recurse(&iter, &iterBloques);

				list_add(archivo->l_bloques, bloque);

			}

			archivo->estado = NO_DISPONIBLE;

			list_add(archivosList, archivo);

		}

		if (mongoc_cursor_error(cursor, &error)) {
			fprintf(stderr, "An error occurred: %s\n", error.message);
		}

	}

	mongoc_cursor_destroy(cursor);
	bson_destroy(query);

}

void actualizarCantidadDeCopiasDB(t_archivo* archivo){

	removeDBArchivo(archivo);
	insertarEnDBArchivo(archivo);

}

void updateArchivoNombre(char* nombreNuevo, char* nombreViejo, int padre){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	BSON_APPEND_UTF8(query, "nombre", nombreViejo);
	BSON_APPEND_INT32(query, "padre", padre);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", updateHijo);
	BSON_APPEND_UTF8(updateHijo, "nombre", nombreNuevo);
	bson_append_document_end(update, updateHijo);

	if (!mongoc_collection_update(collectionArchivo, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}

void updateArchivoPadre(char* nombre, int padreViejo, int padreNuevo){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	BSON_APPEND_UTF8(query, "nombre", nombre);
	BSON_APPEND_INT32(query, "padre", padreViejo);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", updateHijo);
	BSON_APPEND_INT32(updateHijo, "padre", padreNuevo);
	bson_append_document_end(update, updateHijo);

	if (!mongoc_collection_update(collectionArchivo, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

}

void updateArchivoEstado(char* nombre, int padre, int estado){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	BSON_APPEND_UTF8(query, "nombre", nombre);
	BSON_APPEND_INT32(query, "padre", padre);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", updateHijo);
	BSON_APPEND_INT32(updateHijo, "estado", estado);
	bson_append_document_end(update, updateHijo);

	if (!mongoc_collection_update(collectionArchivo, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);


}

void updateArchivoIdNodo(t_archivo* archivo, int bloque, int idNodoViejo, int idNodoNuevo){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	t_list* bloques = archivo->l_bloques;
	t_bloques* bloqueEncontrado = buscarBloquePorIdBloque(bloques, bloque);
	t_list* copias = bloqueEncontrado->copias;
	int copia = buscarCopiaPorIdNodo(copias, idNodoViejo);

	char* cambiarNroBloqueNodo = string_new();
	string_append(&cambiarNroBloqueNodo, "bloques.ubicacionBloque");
	string_append(&cambiarNroBloqueNodo, string_itoa(bloque));
	string_append(&cambiarNroBloqueNodo, ".copia");
	string_append(&cambiarNroBloqueNodo, string_itoa(copia));
	string_append(&cambiarNroBloqueNodo, ".idNodo");

	BSON_APPEND_UTF8(query, "nombre", archivo->nombre);
	BSON_APPEND_INT32(query, "padre", archivo->index_directorio_padre);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", updateHijo);
	BSON_APPEND_INT32(updateHijo, cambiarNroBloqueNodo, idNodoNuevo);
	bson_append_document_end(update, updateHijo);

	if (!mongoc_collection_update(collectionArchivo, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);




}

t_bloques* buscarBloquePorIdBloque(t_list* bloquesList, int id) {

	bool _buscar_bloque_por_id(t_bloques* bloque) {
		return bloque->id_bloque == id;
	}

	return (list_find(bloquesList, (void*) _buscar_bloque_por_id));
}

int buscarCopiaPorIdNodo(t_list* copiasList, int id) {

	int size = list_size(copiasList);
	int posCopia;

	int i;
	for (i = 0; size > i; i++) {

		t_copia* copia = (t_copia*) malloc(sizeof(t_copia));
		copia = list_get(copiasList, i);

		if (copia->id_nodo == id) {
			posCopia = i;
			break;
		}
	}
	return posCopia;

}

void updateArchivoBloqueNodo(t_archivo* archivo, int bloque, int idNodo, int nroBloqueNodoNuevo){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	t_list* bloques = archivo->l_bloques;
	t_bloques* bloqueEncontrado = buscarBloquePorIdBloque(bloques, bloque);
	t_list* copias = bloqueEncontrado->copias;
	int copia = buscarCopiaPorIdNodo(copias, idNodo);

	char* cambiarNroBloqueNodo = string_new();
	string_append(&cambiarNroBloqueNodo, "bloques.ubicacionBloque");
	string_append(&cambiarNroBloqueNodo, string_itoa(bloque));
	string_append(&cambiarNroBloqueNodo, ".copia");
	string_append(&cambiarNroBloqueNodo, string_itoa(copia));
	string_append(&cambiarNroBloqueNodo, ".nroBloqueNodo");

	BSON_APPEND_UTF8(query, "nombre", archivo->nombre);
	BSON_APPEND_INT32(query, "padre", archivo->index_directorio_padre);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", updateHijo);
	BSON_APPEND_INT32(updateHijo, cambiarNroBloqueNodo, nroBloqueNodoNuevo);
	bson_append_document_end(update, updateHijo);


	if (!mongoc_collection_update(collectionArchivo, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);



}

void removeArchivoPorPadre(t_elemento* elemento){

	bson_error_t error;
	bson_t *remove;

	remove = bson_new();

	BSON_APPEND_INT32(remove, "padre", elemento->padre);

	//remueve el registro cuyo id corresponde al de la query
	if(!mongoc_collection_remove(collectionArchivo, MONGOC_REMOVE_SINGLE_REMOVE, remove,
			NULL, &error)){
		printf("%s\n", error.message);
	}

	bson_destroy(remove);

}

void removeDBArchivo(t_archivo* archivo){

	bson_error_t error;
	bson_t *remove;

	remove = bson_new();

	BSON_APPEND_UTF8(remove, "nombre", archivo->nombre);
	BSON_APPEND_INT32(remove, "padre", archivo->index_directorio_padre);

	//remueve el registro cuyo id corresponde al de la query
	if(!mongoc_collection_remove(collectionArchivo, MONGOC_REMOVE_SINGLE_REMOVE, remove,
			NULL, &error)){
		printf("%s\n", error.message);
	}

	bson_destroy(remove);

}

/*
 * Persistencia directorios
 */


void insertarDirectorioDB(t_elemento* elemento) {

	bson_error_t error;
	bson_t *padre = bson_new();

	BSON_APPEND_UTF8(padre, "nombre", elemento->directorio);
	BSON_APPEND_INT32(padre, "index", elemento->index);
	BSON_APPEND_INT32(padre, "indexPadre", elemento->padre);

	if (!mongoc_collection_insert(collectionDirectorio, MONGOC_INSERT_NONE,
			padre, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(padre);

	mongoc_cleanup();

}

void updateNombreDirectorio(t_elemento* elemento, char* nombreNuevo){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	BSON_APPEND_INT32(query, "index", elemento->index);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", updateHijo);
	BSON_APPEND_UTF8(updateHijo, "nombre", nombreNuevo);
	bson_append_document_end(update, updateHijo);

	if (!mongoc_collection_update(collectionDirectorio, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

	mongoc_cleanup();

}

void updatePadreDirectorio(t_elemento* elemento, int padre){

	bson_error_t error;
	bson_t *query;
	bson_t *update;
	bson_t *updateHijo;

	query = bson_new();
	update = bson_new();
	updateHijo = bson_new();

	BSON_APPEND_INT32(query, "index", elemento->index);
	BSON_APPEND_DOCUMENT_BEGIN(update, "$set", updateHijo);
	BSON_APPEND_INT32(updateHijo, "indexPadre", padre);
	bson_append_document_end(update, updateHijo);

	if (!mongoc_collection_update(collectionDirectorio, MONGOC_UPDATE_MULTI_UPDATE,
			query, update, NULL, &error)) {
		printf("%s\n", error.message);
	}

	bson_destroy(query);
	bson_destroy(update);
	bson_destroy(updateHijo);

	mongoc_cleanup();

}

void recuperarDirectorio() {

	mongoc_cursor_t *cursor;
	bson_error_t error;
	const bson_t *doc;
	bson_t *query;
	bson_iter_t iter;

	query = bson_new();

	cursor = mongoc_collection_find(collectionDirectorio, MONGOC_QUERY_NONE, 0,
			0, 0, query, NULL, NULL);

	if (cursor != NULL) {
		while (mongoc_cursor_next(cursor, &doc)) {

			t_elemento* elemento = (t_elemento *) malloc(sizeof(t_elemento));

			bson_iter_init(&iter, doc);
			bson_iter_init_find(&iter, doc, "nombre");
			uint32_t length;
			char* nombre = string_duplicate(bson_iter_utf8(&iter, &length));
			elemento->directorio = nombre;

			bson_iter_init_find(&iter, doc, "index");
			int index = bson_iter_int32(&iter);
			elemento->index = index;

			bson_iter_init_find(&iter, doc, "indexPadre");
			int indexPadre = bson_iter_int32(&iter);
			elemento->padre = indexPadre;

			list_add(mdfs, elemento);

		}

		if (mongoc_cursor_error(cursor, &error)) {
			fprintf(stderr, "An error occurred: %s\n", error.message);
		}

	}
	mongoc_cursor_destroy(cursor);
	bson_destroy(query);

	mongoc_cleanup();

}

void borrarArbolDB(t_elemento* elementoARemover){

	removeDirectorio(elementoARemover);
	removeArchivoPorPadre(elementoARemover);

	t_list* listaHijos = buscarImprimirHijos(elementoARemover, 0);

	int size = list_size(listaHijos);

	int i;
	for(i=0; size > 0; i++){

		removeDirectorio(list_get(listaHijos, i));
		removeArchivoPorPadre(list_get(listaHijos, i));

	}

}

void removeDirectorio(t_elemento* elementoARemover){

	bson_error_t error;
	bson_t *remove;

	remove = bson_new();

	BSON_APPEND_UTF8(remove, "nombre", elementoARemover->directorio);
	BSON_APPEND_INT32(remove, "index", elementoARemover->index);
	BSON_APPEND_INT32(remove, "indexPadre", elementoARemover->padre);


	//remueve el registro cuyo id corresponde al de la query
	if(!mongoc_collection_remove(collectionDirectorio, MONGOC_REMOVE_SINGLE_REMOVE, remove,
			NULL, &error)){
		printf("%s\n", error.message);
	}

	bson_destroy(remove);

	mongoc_cleanup();

}


/*
 * Drops de todas las colecciones
 */

void eliminarCollectionNodoOn(){

	bson_error_t error;
	if (!mongoc_collection_drop(collectionNodoListOn, &error)) {
//		printf("%s\n", error.message);
	}

}

void eliminarCollectionNodoOff(){

	bson_error_t error;
	if (!mongoc_collection_drop(collectionNodoListOff, &error)) {
//		printf("%s\n", error.message);
	}

}

void eliminarCollectionArchivo(){

	bson_error_t error;
	if(!mongoc_collection_drop(collectionArchivo, &error)){
//		printf("%s\n", error.message);
	}

}

void eliminarCollectionVariablesGlobales(){

	bson_error_t error;
	if(!mongoc_collection_drop(collectionGlobales, &error)){
//		printf("%s\n", error.message);
	}

}

void eliminarCollectionDirectorio(){

	bson_error_t error;
	if(!mongoc_collection_drop(collectionDirectorio, &error)){
		//printf("%s\n", error.message);
	}
}

int* inicializarArrayDeBloques(int cant_bloques){

	int* array = malloc (sizeof(int) * cant_bloques);
	int i;
	for (i=0; i<cant_bloques-1; i++){
		array[i]=LIBRE;
	}
	return array;
}

t_nodo* estaNodo(int id) {

	bool _buscar_por_id(t_nodo* nodo) {

		return (nodo->id == id);
	}

	return (list_find(nodosListOff, (void*) _buscar_por_id));

}


void switchNodo(int socket, char* buffer) {

	//ABBBBBBBBBBBBCCCCD
	//A = SOY_NODO
	//B = IP
	//C = PUERTO
	//D = SIZE DATOS (EN GB)
	//E = NODO NUEVO

	char** array = string_split(buffer, ",");

	int id = atoi(array[0]);
	char* ip = array[1];
	char* puerto = array[2];
//	float size_datos_gb = atof(array[3]);
//	int size_datos_mb = size_datos_gb * 1024;
	int size_datos_mb = atoi(array[3]);
	int cant_bloques = size_datos_mb / 20;
	int nodo_nuevo = atoi(array[4]);

	t_nodo* nodo = estaNodo(id);

	if (nodo == NULL) {

		printf("Nodo nuevo conectado. \n");
		log_info(loggerMDFS, "Nodo nuevo conectado.");
		//agregar a la lista
		t_nodo* new_nodo;
		new_nodo = (t_nodo*) malloc(sizeof(t_nodo));
		new_nodo->id = id;
		new_nodo->ip = ip;
		new_nodo->puerto = puerto;
		new_nodo->id_socket = socket;
		new_nodo->size_datos_mb = size_datos_mb;
		new_nodo->bloques_libres = cant_bloques;
		new_nodo->cant_bloques = cant_bloques;
		new_nodo->bloques = inicializarArrayDeBloques(cant_bloques);
		new_nodo->nodo_nuevo = 1;
		list_add(nodosListOff, new_nodo);

	} else {

		nodo->id_socket = socket;
		nodo->ip = ip;
		nodo->puerto = puerto;
		printf("Nodo para reconexión conectado. Socket: %i \n", id);
		log_info(loggerMDFS, "Nodo para reconexión conectado. Socket: %d ", socket);

	}

	free(array);

}

//Busca e imprime(opcional) hijos
t_list* buscarImprimirHijos(t_elemento* elem, bool imprimir) {
	int index_search;
	t_elemento* elemHijo;
	index_search = elem->index;

	//Creo y inicializo lista de encontrados
	t_list *l_encontrados;
	l_encontrados = list_create();

	int size = list_size(mdfs);
	int x;
	for (x = 0; size > x; x++) {
		elemHijo = list_get(mdfs, x);
		if (elemHijo->padre == index_search) {
			list_add(l_encontrados, elemHijo);
		}
	}

	if (imprimir) {
		int i, j;
		j = 0;
		t_elemento* elem_encontrado;
		int size_l_encontrados = list_size(l_encontrados);
		for (i = 0; i < size_l_encontrados; i++) {
			elem_encontrado = list_get(l_encontrados, j);
			printf("%s\n", elem_encontrado->directorio);
			j++;
		}
	}
	return l_encontrados;
	list_destroy(l_encontrados);
}

//void imprimirDirectoriosExistentes(){
//	t_elemento* ptr_aux;// = malloc (sizeof (t_elemento));
//	int size = list_size(mdfs);
//	int i;
//	for(i=0; size>i; i++){
//			ptr_aux = list_get(mdfs, i);
//			buscarImprimirHijos(ptr_aux,true);
//		}
//}

t_elemento* existeDirectorioPorPathCompleto(char* path){

	t_list* camino = armarDir(path);

	int size = list_size(camino);

	return (list_get(camino, size-1));
}

t_elemento* existeDirectorio(char* path){
	t_elemento* ptr_aux;// = malloc (sizeof (t_elemento));

	bool _bucar_por_directorio(t_elemento* elem){
		return (string_equals_ignore_case(elem->directorio, path));
	}

	ptr_aux = list_find(mdfs, (void*)_bucar_por_directorio);

	return ptr_aux;
}

void eliminarElementoMDFS(t_elemento* elemento){

	removeDirectorio(elemento);

	bool _buscar_por_index(t_elemento* elem){
		return (elem->index == elemento->index);
	}

	void _destroy_elementos(t_elemento* elem){

		free(elem->directorio);
		free(elem);

	}

	list_remove_and_destroy_by_condition(mdfs, (void*)_buscar_por_index, (void*)_destroy_elementos);

}

int tamanioArray(char** tabla)
{
	int tam=0;
	while (tabla[tam]!='\0')
		tam++;
	return tam;
}

int tamanioArrayInt(int* tabla)
{
	int tam=0;
	while (tabla[tam]!='\0')
		tam++;
	return tam;
}

//void renombrar(char* location, char* nombre_a_modificar, char* name, t_elemento* ptr_aux){
//
//	char* new_name;
//	new_name = (char*) malloc (128*sizeof(char));
//
//	char** directorios;
//	directorios = string_split(location,"/");
//
//
//	int a=0;
//	while (directorios[a]!=NULL) {
//		strcat(new_name,"/");
//		if(!string_equals_ignore_case(directorios[a], nombre_a_modificar)){
//			strcat(new_name,directorios[a]);
//		}else{
//			strcat(new_name,name);
//		}
//		a++;
//	}
//	ptr_aux->directorio = new_name;
//
//}

char* obtenerNombreUltimoDirectorio(char* path){
	char* nombre;
	nombre = calloc (128, 1);
	char** directorios;
	int b;
	directorios = string_split(path,"/");
	b = tamanioArray(directorios);
	nombre = directorios[b-1];
	return nombre;
}

//int renombrarDirectorio(){
//	char* location;
//	location = (char*) malloc (128*sizeof(char));
//	printf("Ubicacion :");
//	scanf("%s",location);
//
//	t_elemento* ptr_aux = malloc (sizeof (t_elemento));
//	ptr_aux =existeDirectorio(location);
//	if (ptr_aux == NULL){
//		printf("ERROR: El directorio no existe. \n");
//		log_info(loggerMDFS, "ERROR: El directorio no existe.");
//		return -1;
//	}
//
//	char* nombre_a_modificar;
//	nombre_a_modificar = (char*) malloc (128*sizeof(char));
////	char** directorios;
////	int b;
////	directorios = string_split(location,"/");
////	b = tamanioArray(directorios);
////	nombre_a_modificar = directorios[b-1];
//	nombre_a_modificar = obtenerNombreUltimoDirectorio(location);
//
//	char* name;
//	name = (char*) malloc (128*sizeof(char));
//	printf("Nombre nuevo:");
//	scanf("%s",name);
//
//	t_list * l_encontrados;
//	l_encontrados = (t_list *) malloc(sizeof(t_list));
//	l_encontrados = list_create();
//	l_encontrados = buscarImprimirHijos(ptr_aux, false);
//
//	if (list_size(l_encontrados) != 0){
//		actualizarRenombreHijos(l_encontrados,nombre_a_modificar,name);
//	}
//	updateNombreDirectorio(ptr_aux, name);
//	renombrar(location,nombre_a_modificar,name,ptr_aux);
//	log_info(loggerMDFS, "Se ha renombrado el directorio.");
//	printf("Hecho. \n");
//
//	return 0;
//
//}
//
//int moverDirectorio(){
//	char* location;
//	location = (char*) malloc (128*sizeof(char));
//	printf("Ubicacion :");
//	scanf("%s",location);
//
//	t_elemento* ptr_location = malloc (sizeof (t_elemento));
//	ptr_location =existeDirectorio(location);
//	if (ptr_location == NULL){
//		printf("ERROR: El directorio no existe. \n");
//		log_info(loggerMDFS, "ERROR: El directorio no existe.");
//		return -1;
//	}
//
//	char* destino;
//	destino = (char*) malloc (128*sizeof(char));
//	printf("Destino :");
//	scanf("%s",destino);
//
//	t_elemento* ptr_destino = malloc (sizeof (t_elemento));
//	ptr_destino =existeDirectorio(destino);
//	if (ptr_destino == NULL){
//		printf("ERROR: El directorio no existe. \n");
//		log_info(loggerMDFS, "ERROR: El directorio no existe.");
//		return -1;
//	}
//
//	ptr_location->padre = ptr_destino->index;
//	//Ultimo directorio concatenar a destino.
//	char* directorio;
//	directorio = (char*) malloc (256*sizeof(char));
//	char* new_name;
//	new_name = (char*) malloc (256*sizeof(char));
//	directorio = obtenerNombreUltimoDirectorio(location);
//
//	strcpy(new_name,ptr_destino->directorio);
//	strcat(new_name,"/");
//	strcat(new_name,directorio);
////	string_append(&new_name,directorio);
//	ptr_location->directorio = new_name;
//	log_info(loggerMDFS, "Se ha movido el directorio.");
//	printf("Hecho. \n");
//	updatePadreDirectorio(ptr_location, ptr_destino->index);
//
//	return 0;
//}

//char* getDirectorioPorIndex(int index){
//
//	t_elemento* elem;
//
//	bool _buscar_por_index(t_elemento* e1){
//		return (e1->index == index);
//	}
//
//	elem = list_find(mdfs, (void*)_buscar_por_index);
//
//	return elem->directorio;
//}

void imprimirArchivosExistentes(){
	t_archivo* ptr_aux;// =(t_archivo*) malloc (sizeof (t_archivo));
		int i;
		for (i=0; i< list_size(archivosList); i++){
			char* path = string_new();
			char* directorio = string_new();
			ptr_aux = list_get(archivosList,i);
			directorio = getPathCompleto(ptr_aux->index_directorio_padre);
			string_append(&path,directorio);
			if(ptr_aux->index_directorio_padre != 0){
				string_append(&path,"/");
			}
			string_append(&path, ptr_aux->nombre);
			if (ptr_aux->estado == DISPONIBLE){
				printf("%d -%s		DISPONIBLE \n",i+1,path);
			}else{
				printf("%d -%s		NO DISPONIBLE \n",i+1,path);
			}
		}
}

void imprimirArchivosExistentesSinIndice(){
	t_archivo* ptr_aux;// =(t_archivo*) malloc (sizeof (t_archivo));
		int i;
		for (i=0; i< list_size(archivosList); i++){
			char* path = string_new();
			char* directorio = string_new();
			ptr_aux = list_get(archivosList,i);
			directorio = getPathCompleto(ptr_aux->index_directorio_padre);
			string_append(&path,directorio);
			if(ptr_aux->index_directorio_padre != 0){
				string_append(&path,"/");
			}
			string_append(&path, ptr_aux->nombre);
			if (ptr_aux->estado == DISPONIBLE){
				printf("%s		DISPONIBLE \n", path);
			}else{
				printf("%s		NO DISPONIBLE \n", path);
			}
		}
}

t_archivo* buscarArchivo(char* name, int indexPadre){

	bool _buscar_archivo(t_archivo* arch){

		return((arch->index_directorio_padre== indexPadre) && string_equals_ignore_case(arch->nombre, name));
	}

	return( list_find(archivosList, (void*) _buscar_archivo));


}

void renombrarArchivo(char* name, int indexPadre) {

	t_archivo* archivo = buscarArchivo(name, indexPadre);

	if (archivo == NULL) {
		printf("El archivo no existe. \n");
	} else {

		char* new_name = calloc(256, 1);

		printf("\nNombre nuevo: ");
		scanf("%s", new_name);

		if (string_is_empty(new_name)) {
			printf("El nombre nuevo no puede ser vacio. \n");
		}

		updateArchivoNombre(new_name, archivo->nombre,
				archivo->index_directorio_padre);

		archivo->nombre = new_name;

		log_info(loggerMDFS, "Se ha renombrado el archivo.");
		printf("\nHecho.\n");
	}
}

void eliminarArchivo(int indexPadre, char* nombre) {

	int nodoOn;

	bool _buscar_archivo(t_archivo* arch) {

		return ((arch->index_directorio_padre == indexPadre)
				&& string_equals_ignore_case(arch->nombre, nombre));
	}

	t_archivo* archivo = list_remove_by_condition(archivosList,
			(void*) _buscar_archivo);

//	borra los bloques del nodo
	if (archivo == NULL) {

		printf("El archivo que quiere eliminar no existe.");

	} else {

		int i, j;
		for (i = 0; i < list_size(archivo->l_bloques); i++) {
			t_bloques* bloque;
			bloque = list_get(archivo->l_bloques, i);

			for (j = 0; j < list_size(bloque->copias); j++) {
				t_copia* copia = list_get(bloque->copias, j);
				//buscar copia->id_nodo en nodosListOn o en nodosListOff y borrar el bloque

				bool _pertenece_a_lista(t_nodo* n1) {
					return n1->id == copia->id_nodo;
				}

				t_nodo* nodo = list_find(nodosListOn,
						(void*) _pertenece_a_lista);
				nodoOn = 0;
				if (nodo == NULL) {
					nodo = list_find(nodosListOff, (void*) _pertenece_a_lista);
					nodoOn = 1;
				}
				nodo->bloques[copia->bloque] = LIBRE;
				nodo->bloques_libres++;
				if (nodoOn == 0) {
					updateEstadoBloque(copia->id_nodo, copia->bloque, 0,
							collectionNodoListOn);
					updateCantidadBloquesLibres(nodo->id, nodo->bloques_libres,
							collectionNodoListOn);
				} else {
					updateEstadoBloque(copia->id_nodo, copia->bloque, 0,
							collectionNodoListOff);
					updateCantidadBloquesLibres(nodo->id, nodo->bloques_libres,
							collectionNodoListOff);
				}
			}

		}

		removeDBArchivo(archivo);

		//libera memoria
		list_destroy(archivo->l_bloques);

		log_info(loggerMDFS, "Se ha eliminado el archivo %s .",
				archivo->nombre);
		printf("Hecho.\n");
		free(archivo);
	}

}

int moverArchivo(char* nombre, int indexPadre) {

	t_archivo* archivo = buscarArchivo(nombre, indexPadre);

	if(archivo == NULL){
		printf("El archivo no existe.\n");
		return -1;
	}

	printf("!: Desea listar todos los directorios?."
			"[S]í. [N]o. \n");

	char* scanDir = calloc(2, 1);
	scanf("%s", scanDir);
	if (string_equals_ignore_case(scanDir, "S")) {
		listarDirs();
	}

	char* destino = (char*) malloc(256 * sizeof(char));
	printf("Destino :");
	scanf("%s", destino);

	t_list* directorio = armarDir(destino);
	int size = list_size(directorio);
	if (size != 0) {
		t_elemento* dirNuevo = list_get(directorio, size - 1);

		updateArchivoPadre(archivo->nombre, archivo->index_directorio_padre,
				dirNuevo->index);

		archivo->index_directorio_padre = dirNuevo->index;

		log_info(loggerMDFS, "Se ha movido el archivo.");
		printf("\nHecho.\n");
	} else {
		printf("El destino es inexistente. \n");
	}

//	imprimirArchivosExistentes();
	return 0;

}

char* getPathCompleto(int padre) {

	char* path = string_new();

	t_list* camino = list_create();

	while (padre != -1) {

		bool _get_padre(t_elemento* e1) {
			return (e1->index == padre);
		}

		list_add(camino, list_find(mdfs, (void*) _get_padre));

		t_elemento* elemento = getDirectorio(padre);

		padre = elemento->padre;

	}

	int size = list_size(camino);

	if (size == 1) {
		string_append(&path, "/");
	}

	while (size > 0) {

		t_elemento* elem = list_get(camino, size - 1);

		if (elem->index == 0) {

		} else {
			string_append(&path, "/");
			string_append(&path, elem->directorio);

		}

		size--;
	}

	return path;

}

t_elemento* getDirectorio(int index){

	bool _buscar_padre(t_elemento* elem){
		return ((elem->index == index));
	}

	t_elemento* elem = list_find(mdfs, (void*)_buscar_padre);

	return elem;

}

t_elemento* existeDirPorNombre(char* path, int padre){
	t_elemento* ptr_aux;// = malloc (sizeof (t_elemento));

	bool _bucar_por_directorio(t_elemento* elem){
		return (string_equals_ignore_case(elem->directorio, path) && (elem->padre == padre));
	}

	ptr_aux = list_find(mdfs, (void*)_bucar_por_directorio);

	return ptr_aux;
}

int cdDir(int indexPadre, char* path){

	t_elemento* elem;

	if(string_equals_ignore_case(path, "..")) {

			if (indexPadre == 0) {
				printf("Usted ya esta en el root, no puede seguir subiendo.");
			} else {
				 elem = getDirectorio(indexPadre);
				indexPadre = elem->padre;
			}
	}else{

		elem = existeDirectorio(path);
		if(elem!= NULL){
			indexPadre = elem->index;
		}else{
			printf("%s\n", "El directorio no existe.");
		}
	}

	return indexPadre;
}

int renameDir(char* path, int padre){

	t_elemento* elem;

	char* newName = malloc(100);

	bool _existe_en_padre(t_elemento* elem){
		return((elem->padre == padre) && string_equals_ignore_case(elem->directorio, newName));
	}

	elem = existeDirPorNombre(path, padre);
	if(elem!= NULL){
		printf("%s\n", "Ingrese el nuevo nombre para el directorio");
		scanf("%s", newName);
		if (newName == NULL){
			printf("%s\n", "El nombre del directorio no puede estar vacio.");
		} else if(list_any_satisfy(mdfs, (void*)_existe_en_padre)){
			printf("%s\n", "Ya existe un directorio con ese nombre en esta carpeta.");
		}else{
			free(elem->directorio);
			elem->directorio = newName;
			updateNombreDirectorio(elem, newName);
		}
	}else{
		printf("%s\n", "El directorio no existe.");
	}

	return 0;
}

int moverDir(int indexPadre, char* name) {

	t_elemento* dirAMover = existeDirPorNombre(name, indexPadre);

	if (dirAMover != NULL) {

		//	listarDirs();

		printf("%s\n", "Ingrese a donde quiere mover el directorio");
		char* newDir = calloc(30 , 1);
		scanf("%s", newDir);

		t_list* listDirNuevo = armarDir(newDir);

		if (!list_is_empty(listDirNuevo)) {

			int size = list_size(listDirNuevo);

			t_elemento* dirNuevo = list_get(listDirNuevo, size - 1);

			updatePadreDirectorio(dirAMover, dirNuevo->index);
			dirAMover->padre = dirNuevo->index;

		} else {
			printf("%s\n", "El directorio ingresado es inexistente.");
		}

		list_destroy(listDirNuevo);
	} else {
		printf("%s\n", "El directorio que quiere mover no existe.\n");
	}
	return 0;
}

int listarDirs(){

	listarHijos(0, "/");

	return 0;
}

void listarHijos(int padre, char* path){

	t_list* hijos;

	bool _buscar_por_padre(t_elemento* elem){

		return (elem->padre == padre);
	}

	printf("%s\n", path);
	hijos = list_filter(mdfs, (void*) _buscar_por_padre);

	int size = list_size(hijos);

	int i = 0;
	while(i<size){

		t_elemento* elem = list_get(hijos, i);
		char* hijo_path = string_duplicate(path);
		string_append(&hijo_path, elem->directorio);
		string_append(&hijo_path, "/");

		listarHijos(elem->index, hijo_path);
		free(hijo_path);
		i++;
	}

}

t_elemento* encontrarDirectorio(int padre, char* split){


	bool _encontrar_elem(t_elemento* e1) {
		return ((e1->padre == padre)
				&& (string_equals_ignore_case(e1->directorio,
						split)));
	}

	return(list_find(mdfs, (void*) _encontrar_elem));

}

t_archivo* encontrarArchivo(int padre, char* split) {

	bool _encontrar_archivo(t_archivo* arch) {
		return ((arch->index_directorio_padre == padre)
				&& (string_equals_ignore_case(arch->nombre, split)));
	}

	return(list_find(mdfs, (void*) _encontrar_archivo));

}

int getPadre(int dir){

	bool _encontrar_elem(t_elemento* e1) {
		return (e1->index == dir);
	}

	t_elemento* elem = list_find(mdfs, (void*) _encontrar_elem);

	return elem->padre;

}

t_list* armarDir(char* dir) {

	t_list* camino = list_create();

	if (string_equals_ignore_case(dir, "/")) {

		bool _encontrar_elem(t_elemento* e1) {
			return ((e1->index == 0));
		}

		t_elemento* elem;

		elem = list_find(mdfs, (void*) _encontrar_elem);
		list_add(camino, elem);

	} else {

		char** split = string_split(dir, "/");

		int i = 0;
		int padre = 0;

		while (split[i] != NULL) {

			t_elemento* elem = encontrarDirectorio(padre, split[i]);

			if (elem == NULL) {

				t_archivo* archivo = encontrarArchivo(padre, split[i]);

				if (archivo == NULL) {
					printf("El path indicado no existe.\n");
					list_clean(camino);
					break;
				}
			} else {
				list_add(camino, elem);
				padre = elem->index;

			}

			i++;
		}
	}

	return camino;
}

void dirsDondeEstas(int indexPadre){

	printf("\n");

	bool _son_hijos_de(t_elemento* elem) {
		return (elem->padre == indexPadre);
	}

	t_list* hijos = list_filter(mdfs, (void*) _son_hijos_de);

	int size = list_size(hijos);

	int i;
	for (i = 0; size > i; i++) {
		t_elemento* hijo = list_get(hijos, i);
		printf("%s\n", hijo->directorio);
	}

}

int crearDir(int indexPadre, char* newNombre){

//	printf("Ingrese el nombre del nuevo directorio.\n");
//	char* newNombre = string_new();
//	scanf("%s", newNombre);

	bool _existe_en_padre(t_elemento* elem){
		return((elem->padre == indexPadre) && string_equals_ignore_case(elem->directorio, newNombre));
	}

	if(newNombre==NULL){
		printf("%s\n", "El nombre no puede ser vacio.\n");
	}else if(list_any_satisfy(mdfs, (void*)_existe_en_padre)){
		printf("%s\n", "Un directorio con ese nombre ya existe en donde usted esta queriendo crearlo.\n");
	}else{

		t_elemento* elem = (t_elemento*) malloc(sizeof(t_elemento));

		index_mdfs++;
		elem->directorio = newNombre;
		elem->padre = indexPadre;
		elem->index = index_mdfs;

		list_add(mdfs, elem);

		updateGloablesMdfs();
		insertarDirectorioDB(elem);
	}

	return 0;

}

int rmvDirs(int padre, char* path){

	bool _buscar_dir_a_remover(t_elemento* el){
		return ((el->padre == padre) && string_equals_ignore_case(el->directorio, path));
	}

	t_elemento* elementoARemover = list_find(mdfs, (void*)_buscar_dir_a_remover);


	if(elementoARemover == NULL){

		printf("El directorio que quiere eliminar no existe.\n");

	}else{

		removeArchivoPorPadreDir(elementoARemover->index);
		rmvHijos(elementoARemover->index);
		removeDirectorio(elementoARemover);
		removeDir(elementoARemover);

	}

	return 0;
}

void rmvHijos(int padre){

	bool _buscar_por_padre(t_elemento* elem){
		return (elem->padre == padre);
	}

	t_list* listaHijos = list_filter(mdfs, (void*)_buscar_por_padre);

	int size = list_size(listaHijos);

	int i = 0;
	while(i<size){

		t_elemento* elem = list_get(listaHijos, i);

		removeDirectorio(elem);
		rmvHijos(elem->index);
		removeArchivoPorPadreDir(elem->index);
		removeDir(elem);
		i++;
	}

	list_destroy(listaHijos);
}


void removeDir(t_elemento* elementoARemover){

	bool _dir_a_eliminar(t_elemento* elem){
		return (elem->index == elementoARemover->index);
	}

	removeDirectorio(elementoARemover);
	list_remove_and_destroy_by_condition(mdfs, (void*)_dir_a_eliminar, (void*)free);

}

void removeArchivoPorPadreDir(int indexPadre){


	void _archives_destroyer(t_archivo* arch){

//		free(arch->nombre);
		free(arch);

		}


	bool _buscar_por_padre(t_archivo* arch){
		return(arch->index_directorio_padre == indexPadre);
	}

	list_remove_and_destroy_by_condition(archivosList, (void*)_buscar_por_padre, (void*)_archives_destroyer);


}

void imprimirArchivosPorPadre(indexPadre){

	printf("\n");

	bool _buscar_archs_por_padre(t_archivo* arch){
		return(arch->index_directorio_padre == indexPadre);
	}

	t_list* archivosAImprimir = list_filter(archivosList, (void*)_buscar_archs_por_padre);

	int size = list_size(archivosAImprimir);
	int i=0;

	while(i<size){

		t_elemento* elem = list_get(archivosAImprimir, i);

		printf("%s\n", elem->directorio);

		i++;
	}

}

void help(){

	printf("Ingrese la accion que quiera realizar seguida del directorio/archivo sobre el cual quiere aplicarla:\n"
			"\n"
			"Comandos para los directorios:\n"
			"Para recorrer: cd \n"
			"Para crear: crd \n"
			"Para mover: mvd \n"
			"Para renombrar: rnd \n"
			"Para eliminar: rmd \n"
			"\n"
			"Comandos para los archivos:\n"
			"Para mover: mva \n"
			"Para renombrar: rna \n"
			"Para eliminar: rma \n"
			"\n"
			"Para listar lo q se encuentra en el directorio: ls\n"
			"Para listar todo el arbol: tree\n"
			"\n");

}

int buscarDirectorios(int indexPadre) {

	printf("Para saber los comandos disponibles ingrese help.\n"
			"Para volver ingrese 0.\n"
			"-----------------------------------------------------"
			"\n");
	while (1) {

		char* pathActual = getPathCompleto(indexPadre);
		printf("\n%s #>", pathActual);
		free(pathActual);

		char* scan = malloc(100);
		getchar();
		scanf("%[^\n]s", scan);
		char** accion = string_split(scan, " ");

		if (string_equals_ignore_case(accion[0], "0")) {
			return -1;
		}

		if (string_equals_ignore_case(accion[0], "ls")) {

			imprimirArchivosPorPadre(indexPadre);
			dirsDondeEstas(indexPadre);

		} else if (string_equals_ignore_case(accion[0], "cd")) {

			indexPadre = cdDir(indexPadre, accion[1]);

			//crea un directorio nuevo
		} else if (string_equals_ignore_case(accion[0], "crd")) {

			crearDir(indexPadre, accion[1]);

		} else if (string_equals_ignore_case(accion[0], "mvd")) {

			moverDir(indexPadre, accion[1]);

			//renombra el directorio
		} else if (string_equals_ignore_case(accion[0], "rnd")) {

			renameDir(accion[1], indexPadre);

			//elimina el directorio
		} else if (string_equals_ignore_case(accion[0], "rmd")) {

			rmvDirs(indexPadre, accion[1]);

		} else if (string_equals_ignore_case(accion[0], "tree")) {

			listarDirs();
			imprimirArchivosExistentesSinIndice();

		} else if (string_equals_ignore_case(accion[0], "rma")){

			eliminarArchivo(indexPadre, accion[1]);

		} else if (string_equals_ignore_case(accion[0], "rna")){

			renombrarArchivo(accion[1], indexPadre);

		} else if (string_equals_ignore_case(accion[0], "mva")){

			moverArchivo(accion[1], indexPadre);

		}else if(string_equals_ignore_case(accion[0], "help")){

			help();

		} else {
			printf("No es una opcion valida.\n");
		}
	}
	return 0;
}

int menuDos(){

	if (list_size(archivosList) == 0){
		printf("No hay ningún archivo en el mdfs. \n");
		log_info(loggerMDFS, "No hay ningún archivo en el mdfs.");
		return -1;
	}

	printf(
						"Eliminar/Renombrar/Mover archivos :\n"
						"1-Eliminar \n"
						"2-Renombrar \n"
						"3-Mover \n"
						"4-Listar \n"
						"0- Volver \n"
						);

		int scan;
		scanf("%d", &scan);
		printf("\n");
		switch (scan){
		case 0:
			return -1;
			break;
		case 1:
			imprimirArchivosExistentes();
//			eliminarArchivo();

			break;
		case 2:
			imprimirArchivosExistentes();
//			renombrarArchivo();
			break;
		case 3:
			imprimirArchivosExistentes();
//			moverArchivo();
			break;
		case 4:
			imprimirArchivosExistentes();
			break;
		}


		return 0;

}

int menuTres(){

	printf(
					"Crear/Eliminar/Renombrar/Mover directorios :\n"
					"1-Crear \n"
					"2-Eliminar \n"
					"3-Renombrar \n"
					"4-Mover \n"
					"5-Listar \n"
					"6-Recorrer directorio\n"
					"0-Volver \n"
					);

	int scan;
	scanf("%d", &scan);
	printf("\n");
	switch (scan){
	case 0:
		return -1;
		break;
	case 1:

		printf("Elige ubicacion para crear el directorio \n");

		bool _find_root(t_elemento* elem){
			return (elem->index == 0);
		}

		t_elemento* elem = list_find(mdfs, (void*)_find_root);

		printf("%s\n", elem->directorio);
//		imprimirDirectoriosExistentes();
//		crearDirectorio();

		break;
	case 2:
		printf("Elige ubicacion del directorio a eliminar. \n");
//		imprimirDirectoriosExistentes();
//		eliminarDirectorio();
		break;
	case 3:
		printf("Elige ubicacion del directorio a renombrar. \n");
//		imprimirDirectoriosExistentes();
		//renombrarDirectorio();
		break;
	case 4:
		printf("Elija ubicacion del directorio a mover: \n");
//		imprimirDirectoriosExistentes();
		//moverDirectorio();
		break;
	case 5:
//		imprimirDirectoriosExistentes();
		break;
	case 6:
			buscarDirectorios(0);
			break;
	}


	return 0;

}

void removeAllFiles(){
	//borro toda la data de los nodos
	int i;
	for(i=0; i<list_size(nodosListOn); i++){
		t_nodo* nodo = list_get(nodosListOn,i);
		int cant_bloques = nodo->size_datos_mb/20;
		nodo->bloques_libres = cant_bloques;
		int x;
		for(x=0; x<cant_bloques-1; x++){
			nodo->bloques[x] = LIBRE;
//			memset (nodo->bloques, 0, sizeof (int) * cant_bloques);
		}
	}
	//hago lo mismo para nodosListOff
	int j;
	for(j=0; j<list_size(nodosListOff); j++){
		t_nodo* nodo = list_get(nodosListOff,j);
		int cant_bloques = nodo->size_datos_mb/20;
		nodo->bloques_libres = cant_bloques;
		int x;
		for(x=0; x<cant_bloques-1; x++){
			nodo->bloques[x] = LIBRE;
//			memset (nodo->bloques, 0, sizeof (int) * cant_bloques);
		}
	}
}

void liberarNodos(t_list* listNodos, mongoc_collection_t* collection){

	int size = list_size(listNodos);
	int i;
	for(i=0; i < size; i++){

		t_nodo* nodo = list_get(listNodos, i);

		int tamanio = tamanioArrayInt(nodo->bloques);

		int t;
		for (t=0; t < tamanio; t ++){

			nodo->bloques[t] = 0;

		}

		removeNodoDB(nodo->id, collection);
		insertarEnDBNodo(nodo, collection);


	}

}

int formatear() {

	printf("!: El filesystem se va a formatear."
			" Continuar? \n"
			"[S]í. [N]o. \n");

	char* scan = calloc(2,1);
	scanf("%s", scan);
	if (string_equals_ignore_case(scan, "S")) {
		if (!list_is_empty(mdfs)) {

			list_destroy_and_destroy_elements(mdfs, (void*)free);

			eliminarCollectionDirectorio();
			eliminarCollectionArchivo();
			eliminarCollectionVariablesGlobales();

			removeAllFiles();
			list_destroy_and_destroy_elements(archivosList, (void*)free) ;

			liberarNodos(nodosListOff, collectionNodoListOff);
			liberarNodos(nodosListOn, collectionNodoListOn);

			crearFilesystem();

			log_info(loggerMDFS, "Se ha formateado el mdfs.");
			printf("\n Hecho. \n");
			free(scan);
			return 0;
		}
	}
	free(scan);
	return -1;
}

void ordenarNodosList(t_list* nodosList){

	bool _mayor_bloques_libres (t_nodo* n1, t_nodo* n2) {
		return n1->bloques_libres > n2->bloques_libres;
	}

	list_sort(nodosList, (void*)_mayor_bloques_libres);
}

int getPrimerBloqueLibre(t_nodo* nodo){

	//se da por asumido que el nodo tiene al menos un bloque libre (ya se verifico en nodosDisponibles())
	int cant_bloques;
	cant_bloques = nodo->size_datos_mb/20;
	int i;
	i=0;
	while (nodo->bloques[i] == OCUPADO && i<cant_bloques-1){
		i++;
	}
	return i;
}

int enviarDatosANodos(char* map, t_archivo* archivo,int num_bloque, int retAcum){

	ordenarNodosList(nodosListOn);

	t_nodo* nodo_copia_1 = list_get(nodosListOn,0);
	t_nodo* nodo_copia_2 = list_get(nodosListOn,1);
	t_nodo* nodo_copia_3 = list_get(nodosListOn,2);

	int bloque_a_grabar_copia_1;
	bloque_a_grabar_copia_1 = getPrimerBloqueLibre(nodo_copia_1);
	int bloque_a_grabar_copia_2;
	bloque_a_grabar_copia_2 = getPrimerBloqueLibre(nodo_copia_2);
	int bloque_a_grabar_copia_3;
	bloque_a_grabar_copia_3 = getPrimerBloqueLibre(nodo_copia_3);

	printf("\nEnviando bloque N° %d a bloques ID: %d,%d,%d  ...", num_bloque, nodo_copia_1->id, nodo_copia_2->id,nodo_copia_3->id);
	log_info(loggerMDFS, "Enviando bloque N° %d a bloques ID: %d,%d,%d  ...", num_bloque, nodo_copia_1->id, nodo_copia_2->id,nodo_copia_3->id);

	nodo_copia_1->bloques[bloque_a_grabar_copia_1] = OCUPADO;
	nodo_copia_2->bloques[bloque_a_grabar_copia_2] = OCUPADO;
	nodo_copia_3->bloques[bloque_a_grabar_copia_3] = OCUPADO;

	char* buffer_1 = string_new();
	char* buffer_2 = string_new();
	char* buffer_3 = string_new();

	string_append(&buffer_1,SET_BLOQUE);
	string_append(&buffer_2,SET_BLOQUE);
	string_append(&buffer_3,SET_BLOQUE);

	string_append(&buffer_1,string_itoa(bloque_a_grabar_copia_1));
	string_append(&buffer_1, ",");
	string_append(&buffer_2,string_itoa(bloque_a_grabar_copia_2));
	string_append(&buffer_2, ",");
	string_append(&buffer_3,string_itoa(bloque_a_grabar_copia_3));
	string_append(&buffer_3, ",");

	//enviar hasta el ultimo /n del bloque
//	char* map_char = string_new();

	int bytesToSend = (archivo->size_bytes) - retAcum;

	char* prox = memrchr(map,'\n', bytesToSend >= 20*MB ? BLOCKSIZE: bytesToSend);

	int buffer_sendsize;

	if(bytesToSend >= 20*MB)
		buffer_sendsize = prox - map + 1;
	else
		buffer_sendsize = bytesToSend;

	int cant_digitos = calcularCantDigitos(buffer_sendsize);

	string_append(&buffer_1,string_itoa(cant_digitos));
	string_append(&buffer_1, ",");
	string_append(&buffer_2,string_itoa(cant_digitos));
	string_append(&buffer_2, ",");
	string_append(&buffer_3,string_itoa(cant_digitos));
	string_append(&buffer_3, ",");


	string_append(&buffer_1,string_itoa(buffer_sendsize));
	string_append(&buffer_2,string_itoa(buffer_sendsize));
	string_append(&buffer_3,string_itoa(buffer_sendsize));

	int strlenBuffer1 = strlen(buffer_1);
	int strlenBuffer2 = strlen(buffer_2);
	int strlenBuffer3 = strlen(buffer_3);


//		int cant_send,i,resto;
//		cant_send = buffer_sendsize / 1400; //1400 = SENDSIZE
//		resto = buffer_sendsize % 1400;
//		if(resto != 0){
//			cant_send++;
//		}
//
//		for(i=1; i<=cant_send;i++){
//			sendall(nodo_copia_1->id_socket,map,1400);
//			sendall(nodo_copia_2->id_socket,map,1400);
//			sendall(nodo_copia_3->id_socket,map,1400);
//			map = map + 1400;
//			if(i==cant_send){
//				//resto de la divicion
//				sendall(nodo_copia_1->id_socket,map,resto);
//				sendall(nodo_copia_2->id_socket,map,resto);
//				sendall(nodo_copia_3->id_socket,map,resto);
//			}
//		}


	//envio SOY_FS-bytesAEnviar
	sendHeader(SOY_FS,strlenBuffer1,nodo_copia_1->id_socket);
	sendHeader(SOY_FS,strlenBuffer2,nodo_copia_2->id_socket);
	sendHeader(SOY_FS,strlenBuffer3,nodo_copia_3->id_socket);

	//envio buffer
	enviar(nodo_copia_1->id_socket, buffer_1,strlen(buffer_1));
	enviar(nodo_copia_2->id_socket, buffer_2,strlen(buffer_2));
	enviar(nodo_copia_3->id_socket, buffer_3,strlen(buffer_3));

	sendall(nodo_copia_1->id_socket,map,buffer_sendsize);	//cuidadito
	sendall(nodo_copia_2->id_socket,map,buffer_sendsize);
	sendall(nodo_copia_3->id_socket,map,buffer_sendsize);

	//guardo en archivosList
	t_bloques* bloque = (t_bloques*) malloc (sizeof(t_bloques));
	bloque->copias = list_create();
	bloque->id_bloque = num_bloque;
	bloque->tamanio_grabado = buffer_sendsize;

	t_copia* copia1 = (t_copia*) malloc(sizeof(t_copia));
	t_copia* copia2 = (t_copia*) malloc(sizeof(t_copia));
	t_copia* copia3 = (t_copia*) malloc(sizeof(t_copia));

	copia1->id_nodo = nodo_copia_1->id;
	copia2->id_nodo = nodo_copia_2->id;
	copia3->id_nodo = nodo_copia_3->id;

	copia1->nodo_socket = nodo_copia_1->id_socket;
	copia2->nodo_socket = nodo_copia_2->id_socket;
	copia3->nodo_socket = nodo_copia_3->id_socket;

	copia1->bloque = bloque_a_grabar_copia_1;
	copia2->bloque = bloque_a_grabar_copia_2;
	copia3->bloque = bloque_a_grabar_copia_3;

	copia1->nodo_online = DISPONIBLE;
	copia2->nodo_online = DISPONIBLE;
	copia3->nodo_online = DISPONIBLE;

	list_add(bloque->copias,copia1);
	list_add(bloque->copias,copia2);
	list_add(bloque->copias,copia3);

	list_add(archivo->l_bloques,bloque);

	//actualiza listaNodos
	nodo_copia_1->bloques_libres--;
	nodo_copia_2->bloques_libres--;
	nodo_copia_3->bloques_libres--;

	updateEstadoBloque(copia1->id_nodo, copia1->bloque, 1, collectionNodoListOn);
	updateEstadoBloque(copia2->id_nodo, copia2->bloque, 1, collectionNodoListOn);
	updateEstadoBloque(copia3->id_nodo, copia3->bloque, 1, collectionNodoListOn);

	updateCantidadBloquesLibres(nodo_copia_1->id, nodo_copia_1->bloques_libres, collectionNodoListOn);
	updateCantidadBloquesLibres(nodo_copia_2->id, nodo_copia_2->bloques_libres, collectionNodoListOn);
	updateCantidadBloquesLibres(nodo_copia_3->id, nodo_copia_3->bloques_libres, collectionNodoListOn);

	printf("OK.");
	free(buffer_1);
	free(buffer_2);
	free(buffer_3);

	return buffer_sendsize;
}

//ordena nodosList por mayor cant de bloques libres,
//retorna -1 en caso de que no haya espacio libre en los nodos para grabar
int nodosDisponibles(int cant_bloques) {

	if(list_size(nodosListOn) < 3){
		printf("No se puede grabar porque hay menos de 3 nodos disponibles\n");
		return -1;
	}

	t_list* listaAux = list_create();

	int n;
	for (n = 0; list_size(nodosListOn) >n ; n++) {
		t_nodo* nodoListOn = list_get(nodosListOn, n);
		t_nodo* new_nodo;
		new_nodo = (t_nodo*) malloc(sizeof(t_nodo));
		new_nodo->id = nodoListOn->id;
		new_nodo->ip = nodoListOn->ip;
		new_nodo->puerto = nodoListOn->puerto;
		new_nodo->id_socket = nodoListOn->id_socket;
		new_nodo->size_datos_mb = nodoListOn->size_datos_mb;
		new_nodo->bloques_libres = nodoListOn->bloques_libres;
		new_nodo->bloques = nodoListOn->bloques;

		list_add(listaAux, new_nodo);
	}

	t_nodo* nodo;

	int i;
	for (i = 0; cant_bloques > i; i++) {

		ordenarNodosList(listaAux);	//LA LISTA TIENE QUE QUEDAR ORDENADA!!! EN ENVIARDATOSANODO USA LA LISTA ORDENADA!!

		int x;
		for (x = 0; 3 > x; x++) {

			nodo = list_get(listaAux, x);

			if (nodo->bloques_libres > 0) {
				nodo->bloques_libres--;

			} else {
				printf(
						"\nERROR: No hay suficientes bloques libres para grabar este archivo.");
				log_info(loggerMDFS, "ERROR: No hay suficientes bloques libres para grabar este archivo.");
				list_destroy_and_destroy_elements(listaAux, free);
				return -1;
			}
		}

	}

	list_destroy_and_destroy_elements(listaAux, free);

	return 0;
}

int getCantidadBloquesArchivo(int size_bytes){
	//20MB es el valor maximo del bloque
	int cant_bloques;
//	int size_bytes;
//	size_bytes = size_mb * 1024;
	cant_bloques = size_bytes/(20*MB);
	if(size_bytes % (20*MB) != 0){ //167772160bytes = 20MB, el archivo ocupa un solo bloque,
		cant_bloques++;			//size%20 es tan chico que devuelve 0.
	}
	return cant_bloques;
}

t_archivo* getArchivo(char* name){
	bool _obtener_por_nombre(t_archivo* arch){
		return string_equals_ignore_case(name,arch->nombre);
	}

	return list_find(archivosList,(void*) _obtener_por_nombre);
}

char* getPathArchivo(char* location){
	char* path = string_new();
	char** directorios;
	int b;
	directorios = string_split(location,"/");
	b = tamanioArray(directorios);	// /user/jns/archivos/algo.txt
									//	[user,jns,archivos,algo.txt]
	int i;
	for(i=0; i<b-1; i++){
		string_append(&path,"/");
		string_append(&path,directorios[i]);
	}
	if(b==1){
		string_append(&path,"/");
	}

	return path;
}

void imprimirBloques(t_archivo* archivo){

	int cant_bloques_grabados;
	cant_bloques_grabados = list_size(archivo->l_bloques);
	int cant_bloques_totales;
	cant_bloques_totales = getCantidadBloquesArchivo(archivo->size_bytes);

	int i;
	int j;
	for(i=0;i<cant_bloques_grabados; i++){
		t_bloques* bloque;
		bloque = list_get(archivo->l_bloques,i);
		int cant_copias;
		cant_copias = list_size(bloque->copias);
		printf("\n");
		printf("\nBLOQUE: %d CANTIDAD DE COPIAS: %d", bloque->id_bloque, cant_copias);
		for(j=0; j<cant_copias; j++){
			t_copia* copia;
			copia = list_get(bloque->copias,j);
			printf("\nCOPIA: %d ID NODO: %d BLOQUE: %d",j+1,copia->id_nodo,copia->bloque);
		}
	}

	printf("\n");
	printf("\nCantidad de bloques grabados del archivo %d / %d",cant_bloques_grabados,cant_bloques_totales);
	log_info(loggerMDFS, "Cantidad de bloques grabados del archivo %d / %d",cant_bloques_grabados,cant_bloques_totales);

}

int borrarBloques(){

	imprimirArchivosExistentes();

	printf("\n0 - Volver \n");
	printf("Seleccione el índice del archivo : ");
	int scan;
	scanf("%d",&scan);

	if (scan == 0){
		return -1;
	}

	if(scan-1 > list_size(archivosList)){
		printf("\nERROR: no existe el índice %d .",scan);
		log_info(loggerMDFS,"ERROR: no existe el índice %d .",scan);
		return -1;
	}

	t_archivo* archivo = list_get(archivosList,scan-1);

	imprimirBloques(archivo);

	printf("\nN° de bloque a borrar : ");
	int num_bloque;
	scanf("%d",&num_bloque);

	t_bloques* bloque;

	bool _bloque_por_numero (t_bloques* t1){
		return t1->id_bloque == num_bloque;
	}
	bloque = list_find(archivo->l_bloques, (void*) _bloque_por_numero);
	if(bloque == NULL){
		printf("\nERROR: no existe el N° de bloque ingresado");
		log_info(loggerMDFS, "ERROR: no existe el N° de bloque ingresado");
		return -1;
	}

	printf("\nN° de copia : ");
	int num_copia;
	scanf("%d",&num_copia);

	t_copia* copia;
	copia = list_remove(bloque->copias, num_copia-1);
	if(copia == NULL){
		printf("\nERROR: no existe el N° de copia ingresado para el bloque N° %d",num_bloque);
		log_info(loggerMDFS, "ERROR: no existe el N° de copia ingresado para el bloque N° %d",num_bloque);
		return -1;
	}
	//actualizo array de nodo
	bool _pertenece_a_lista (t_nodo* n1) {
		return n1->id == copia->id_nodo;
	}

	t_nodo* nodo = list_find(nodosListOn, (void*) _pertenece_a_lista);
	if (nodo == NULL){
		nodo = list_find(nodosListOff, (void*) _pertenece_a_lista);
	}
	nodo->bloques[copia->bloque] = LIBRE;
	nodo->bloques_libres++;

	//checkeo si hay que deshabilitar el archivo
	if(list_size(bloque->copias) == 0){
		bool _bloque_por_numero (t_bloques* t1){
			return t1->id_bloque == num_bloque;
		}
		list_remove_by_condition(archivo->l_bloques, (void*) _bloque_por_numero);
		archivo->estado = NO_DISPONIBLE;
		printf("\nEl estado del archivo %s es: NO DISPONIBLE", archivo->nombre);
		log_info(loggerMDFS,"El estado del archivo %s es: NO DISPONIBLE", archivo->nombre);
	}

	actualizarCantidadDeCopiasDB(archivo);

	printf("\n");
	log_info(loggerMDFS,"Se ha borrado el bloque.");
	printf("\nHecho.");

	return 0;
}

int verBloques(){

	imprimirArchivosExistentes();

	printf("\n0 - Volver \n");
	printf("Seleccione el índice del archivo a ver : ");
	int scan;
	scanf("%d",&scan);

	if (scan == 0){
		return -1;
	}

	if(scan-1 > list_size(archivosList)){
		printf("\nERROR: no existe el índice %d .",scan);
		log_info(loggerMDFS,"ERROR: no existe el índice %d .",scan);
		return -1;
	}

	t_archivo* archivo = list_get(archivosList,scan-1);

	imprimirBloques(archivo);

	return 0;
}

int copiarBloques(){

	imprimirArchivosExistentes();

	printf("\n0 - Volver \n");
	printf("Seleccione el índice del archivo : ");
	int scan;
	scanf("%d",&scan);

	if (scan == 0){
		return -1;
	}

	if(scan-1 > list_size(archivosList)){
		printf("\nERROR: no existe el índice %d .",scan);
		log_info(loggerMDFS,"ERROR: no existe el índice %d .",scan);
		return -1;
	}

	t_archivo* archivo = list_get(archivosList,scan-1);

	if(archivo->estado == NO_DISPONIBLE){
		printf("\nEl archivo seleccionado se encuentra NO DISPONIBLE.");
		log_info(loggerMDFS,"El archivo seleccionado se encuentra NO DISPONIBLE.");
		return -1;
	}

	imprimirBloques(archivo);

	printf("\nN° de bloque : ");
	int num_bloque;
	scanf("%d",&num_bloque);

	t_bloques* bloque;

	bool _bloque_por_numero (t_bloques* t1){
		return t1->id_bloque == num_bloque;
	}
	bloque = list_find(archivo->l_bloques, (void*) _bloque_por_numero);
	if(bloque == NULL){
		printf("\nERROR: no existe el N° de bloque ingresado");
		log_info(loggerMDFS,"ERROR: no existe el N° de bloque ingresado");
		return -1;
	}

	//pedirle el bloque a la primer copia con nodo conectado
	int j=0;
	int socket_nodo_copia_a_pedir = -1;
	int bloque_a_pedir;
	while(socket_nodo_copia_a_pedir == -1){
		t_copia* copia = list_get(bloque->copias,j);
		if (copia->nodo_online == 1){
			socket_nodo_copia_a_pedir = copia->nodo_socket;
			bloque_a_pedir = copia->bloque;
		}
		j++;	//OJO
	}

	//imprimo nodos conectados
	printf("\nSeleccione el índice del nodo destino :\n");
	int i;
	for (i=0; i<list_size(nodosListOn); i++){
		t_nodo* nodo = list_get(nodosListOn,i);
		printf("%d - ID: %d - IP: %s PUERTO: %s CANTIDAD BLOQUES LIBRES: %d\n",i+1,nodo->id,nodo->ip,nodo->puerto, nodo->bloques_libres);
	}

	int scan2;
	scanf("%d",&scan2);

	if(scan2-1 > list_size(nodosListOn)){
		printf("\nERROR: no existe el índice %d .",scan);
		log_info(loggerMDFS,"ERROR: no existe el índice %d .",scan);
		return -1;
	}

	t_nodo* nodo_destino = list_get(nodosListOn,scan2-1);

	//recibir de nodo
	char* buffer = string_new();
	string_append(&buffer,GET_BLOQUE);
	string_append(&buffer,string_itoa(bloque_a_pedir));
	string_append(&buffer,",");

	int strLenBuffer = strlen(buffer);
	sendHeader(SOY_FS,strLenBuffer,socket_nodo_copia_a_pedir);
	enviar(socket_nodo_copia_a_pedir,buffer,strLenBuffer);

	sem_wait(&mutexFork);
//	map_bloque y map_bloque_size estan seteados

	int num_bloque_a_grabar = getPrimerBloqueLibre(nodo_destino);

	char* buffer_1 = string_new();
	char* data = string_new();
	data = (char*) map_bloque;
	int buffer_sendsize = bloque->tamanio_grabado;

	string_append(&buffer_1,SET_BLOQUE);
	string_append(&buffer_1,string_itoa(num_bloque_a_grabar));
	string_append(&buffer_1, ",");
	int cant_digitos = calcularCantDigitos(buffer_sendsize);
	string_append(&buffer_1,string_itoa(cant_digitos));
	string_append(&buffer_1, ",");
	string_append(&buffer_1,string_itoa(buffer_sendsize));

	int strlenBuffer1 = strlen(buffer_1);

	//envio SOY_FS-bytesAEnviar
	sendHeader(SOY_FS,strlenBuffer1,nodo_destino->id_socket);

	//envio buffer
	enviar(nodo_destino->id_socket, buffer_1,strlen(buffer_1));

	//envio data
	sendall(nodo_destino->id_socket,data,buffer_sendsize);

	//creo copia
	t_copia* new_copia = (t_copia*) malloc(sizeof(t_copia));
	new_copia->bloque = num_bloque_a_grabar;
	new_copia->id_nodo = nodo_destino->id;
	new_copia->nodo_online = 1;
	new_copia->nodo_socket = nodo_destino->id_socket;

	//agrego copia
	list_add(bloque->copias,new_copia);

	//actualizo nodo
	nodo_destino->bloques[num_bloque_a_grabar] = OCUPADO;
	nodo_destino->bloques_libres--;

	actualizarCantidadDeCopiasDB(archivo);

	log_info(loggerMDFS,"Se ha copiado el bloque.");
	printf("\nHecho.");

	return 0;
}

int menuSiete(){
	printf(
					"Ver/Borrar/Copiar bloques de archivos: \n"
					"1-Ver \n"
					"2-Borrar\n"
					"3-Copiar \n"
					"0-Volver \n"
					);

	int scan;
	scanf("%d", &scan);
	printf("\n");
	switch (scan){
		case 0:
			return -1;
			break;
		case 1: verBloques();
			break;
		case 2: borrarBloques();
			break;
		case 3: copiarBloques();
			break;
	}

	return 0;

}

t_archivo* existeArchivo(char* name, int dir){

	bool _encontrar_arch(t_archivo* arch) {
		return ((arch->index_directorio_padre==dir) && (string_equals_ignore_case(arch->nombre, name)));
	}

	return(list_find(archivosList, (void*) _encontrar_arch));

}

int existeNombreArchivo(char* location, int dir) {

	char** split = string_split(location, "/");

	int size = tamanioArray(split);

	char* nombreArch = split[size-1];

	bool _encontrar_arch(t_archivo* arch) {
		return ((arch->index_directorio_padre==dir) && (string_equals_ignore_case(arch->nombre, nombreArch)));
	}

	return(list_any_satisfy(archivosList, (void*) _encontrar_arch));


}

int importarArchivo() {

	char* location = (char*) malloc(128 * sizeof(char));
	printf("Archivo a copiar :");
	scanf("%s", location);
	FILE* file;
	if (!(file = fopen(location, "r"))) {
		perror("El archivo no existe o no se pudo abrir.");
	} else {

		char* destino;
		destino = (char*) malloc(128 * sizeof(char));
		printf("Destino :");
		scanf("%s", destino);

		t_list* caminoDirectorio = armarDir(destino);
		if (list_is_empty(caminoDirectorio)) {
			printf("ERROR: El directorio no existe. \n");
			log_info(loggerMDFS, "ERROR: El directorio no existe.");
			fclose(file);
			return -1;

		} else {

			int size = list_size(caminoDirectorio);
			t_elemento* dir = list_get(caminoDirectorio, size - 1);

			if(existeNombreArchivo(location, dir->index)){

				printf("Ya existe un archivo con ese nombre en este directorio");
				return -1;

			}

			int size_bytes;
			fseek(file, 0, SEEK_END);
			//ftell retorna valor en bytes
			size_bytes = ftell(file);	//size en bytes
			rewind(file);	//nos situa en el comienzo del archivo

			int cant_bloques;
			cant_bloques = getCantidadBloquesArchivo(size_bytes);

			if (nodosDisponibles(cant_bloques) == -1) {
				fclose(file);
				return -1;
			}

			t_archivo* new_archivo = (t_archivo*) malloc(sizeof(t_archivo));
			char* name = string_new();
			//esta funcion tambien sirve para paths a archivos.
			name = obtenerNombreUltimoDirectorio(location);
			new_archivo->nombre = name;
			new_archivo->l_bloques = list_create();
			new_archivo->size_bytes = size_bytes;
			new_archivo->index_directorio_padre = dir->index;

			int ret;
			int retAcum = 0;

			printf("\nMapeando archivo...      ");
			log_info(loggerMDFS, "Mapeando archivo...      ");
			char* map; //map del archivo completo
			if ((map = mmap(NULL, size_bytes, PROT_READ, MAP_SHARED,
					fileno(file), 0)) == MAP_FAILED) {
				perror("ERROR_MAP");
				exitError("MAP_FAILED");
			}
			log_info(loggerMDFS, "OK.");
			printf("OK.");
			int j;
			for (j = 0; j < cant_bloques; j++) {

				ret = enviarDatosANodos((map + retAcum), new_archivo, j,
						retAcum);
				retAcum += ret;
				log_info(loggerMDFS,"Enviados totales %d/%d\n", retAcum,size_bytes);

			}

			log_info(loggerMDFS, "Grabó %d/%d", retAcum, size_bytes);

			new_archivo->estado = DISPONIBLE;
			list_add(archivosList, new_archivo);

			insertarEnDBArchivo(new_archivo);

			munmap(map, size_bytes);
			free(location);
			free(destino);
			fclose(file);
		}
	}

	return 0;
}

int checkearArchivosActivados(t_nodo* nodo) {
	//busco los archivos que usen al nodo
	int i, x, z;
	for (i = 0; i < list_size(archivosList); i++) {
		t_archivo* archivo = list_get(archivosList, i);
		bool copias_disponibles = true;
		for (x = 0; x < list_size(archivo->l_bloques); x++) {
			t_bloques* bloque = list_get(archivo->l_bloques, x);

			//copias por cada num_bloque
			for (z = 0; z < list_size(bloque->copias); z++) {
				t_copia* copia = list_get(bloque->copias, z);
				if (copia->id_nodo == nodo->id) {
					copia->nodo_online = DISPONIBLE;
					copia->nodo_socket = nodo->id_socket;
				}

			}
			bool _al_menos_una_copia_online(t_copia* copia) {
				return copia->nodo_online == 1;
			}

			if (list_any_satisfy(bloque->copias,
					(void*) _al_menos_una_copia_online) && copias_disponibles) {
				copias_disponibles = true;
			} else {
				copias_disponibles = false;
			}
		}
		int cant_bloques = getCantidadBloquesArchivo(archivo->size_bytes);
		int cant_bloques_grabados = list_size(archivo->l_bloques);
		//puede ser que el archivo se encuentre NO_DISPONIBLE porque se haya eliminado algun bloque desde consola y no por desconexion de nodo.
		if (copias_disponibles && (cant_bloques == cant_bloques_grabados)) {
			if (archivo->estado != DISPONIBLE) {
				archivo->estado = DISPONIBLE;
				printf("\nEl archivo %s ahora se encuentra disponible.",
						archivo->nombre);
				log_info(loggerMDFS,
						"\nEl archivo %s ahora se encuentra disponible.",
						archivo->nombre);
			}

		}
	}
	return 0;
}

int agregarNodo() {

	t_list* nodosParaAgregar = list_create();

	printf("\nLista de nodos preparados para agregar:\n");

	//listar nodos listos para dar de alta
	t_nodo* nodo;
	int i;
	for (i = 0; i < list_size(nodosListOff); i++) {
		nodo = list_get(nodosListOff, i);
		if (nodo->id_socket != -1) {
			nodo = list_get(nodosListOff, i);
			list_add(nodosParaAgregar, nodo);
		}
	}

	int j;
	for (j = 0; list_size(nodosParaAgregar) > j; j++) {
		nodo = list_get(nodosParaAgregar, j);
		if ((nodo->nodo_nuevo == 1)) {
			printf(
					"%d - ESTADO: NUEVO - IP: %s PUERTO: %s CANTIDAD BLOQUES LIBRES: %d\n",
					j + 1, nodo->ip, nodo->puerto, nodo->bloques_libres);
		} else {
			printf(
					"%d - ESTADO: RECONEXION ID: %d IP: %s PUERTO: %s CANTIDAD BLOQUES LIBRES: %d\n",
					j + 1, nodo->id, nodo->ip, nodo->puerto,
					nodo->bloques_libres);
		}
	}

	if (list_size(nodosParaAgregar) == 0) {
		printf("La lista de nodos a agregar esta vacia.\n");
		return -1;

	}
	printf("\n0 - Volver \n");
	printf(
			"Seleccione el índice del nodo a agregar o ingrese T para agregar todos.\n");
	char* scan = calloc(3, 1);
	scanf("%s", scan);

	if (string_equals_ignore_case(scan, "0")) {
		return -1;
	}

	if (string_equals_ignore_case(scan, "t")) {

		int i;
		for (i = 0; list_size(nodosParaAgregar) > i; i++) {

			nodo = list_get(nodosParaAgregar, i);
			if (nodo->nodo_nuevo == 1) {
				nodo->nodo_nuevo = 0;
			} else {
				checkearArchivosActivados(nodo);
			}

			list_add(nodosListOn, nodo);
			insertarEnDBNodo(nodo, collectionNodoListOn);

			bool _buscar_por_id(t_nodo* n1) {

				return (n1->id == nodo->id);

			}

			list_remove_by_condition(nodosListOff, (void*) _buscar_por_id);
			removeNodoDB(nodo->id, collectionNodoListOff);

			if (socketMarta != -1) {
				char* mensaje = string_new();
				string_append(&mensaje, "1"); //'1' : enviando nodo conectado a MaRTA
				string_append(&mensaje, string_itoa(nodo->id));
				string_append(&mensaje, ",");
				string_append(&mensaje, nodo->ip);
				string_append(&mensaje, ",");
				string_append(&mensaje, nodo->puerto);

				int strlenMensaje = strlen(mensaje);

				sendHeader(SOY_FS, strlenMensaje, socketMarta);
				enviar(socketMarta, mensaje, strlen(mensaje));
				printf("\n El id del nodo agregado es: %d\n", nodo->id);
				log_info(loggerMDFS, "Se ha agregado el nodo id: %d", nodo->id);
			}

			if (list_size(nodosListOn) >= LISTA_NODOS && estado != OPERATIVO) {
				estado = OPERATIVO;
				printf("\nSistema en modo OPERATIVO.");
				log_info(loggerMDFS, "Sistema en modo OPERATIVO.");
			}
		}

	} else {
		if (atoi(scan) - 1 > list_size(nodosParaAgregar)) {
			printf("\nEl nodo seleccionado no existe");
		} else {

			nodo = list_get(nodosParaAgregar, atoi(scan) - 1);
			if (nodo->nodo_nuevo == 1) {
				nodo->nodo_nuevo = 0;
			} else {
				checkearArchivosActivados(nodo);
			}

			list_add(nodosListOn, nodo);
			insertarEnDBNodo(nodo, collectionNodoListOn);

			bool _buscar_por_id(t_nodo* n1) {

				return (n1->id == nodo->id);

			}

			list_remove_by_condition(nodosListOff, (void*) _buscar_por_id);
			removeNodoDB(nodo->id, collectionNodoListOff);

			if (socketMarta != -1) {
				char* mensaje = string_new();
				string_append(&mensaje, "1"); //'1' : enviando nodo conectado a MaRTA
				string_append(&mensaje, string_itoa(nodo->id));
				string_append(&mensaje, ",");
				string_append(&mensaje, nodo->ip);
				string_append(&mensaje, ",");
				string_append(&mensaje, nodo->puerto);

				int strlenMensaje = strlen(mensaje);

				sendHeader(SOY_FS, strlenMensaje, socketMarta);
				enviar(socketMarta, mensaje, strlen(mensaje));
				printf("\n El id del nodo agregado es: %d\n", nodo->id);
				log_info(loggerMDFS, "Se ha agregado el nodo id: %d", nodo->id);
			}

			if (list_size(nodosListOn) >= LISTA_NODOS && estado != OPERATIVO) {
				estado = OPERATIVO;
				printf("\nSistema en modo OPERATIVO.");
				log_info(loggerMDFS, "Sistema en modo OPERATIVO.");
			}
		}
	}
	return 0;

}

void checkearArchivosDesactivados(t_nodo* nodo) {
	//busco los archivos que usen al nodo
	bool flag = false; //para evitar que avise 752 veces que se desactivo un archivo
	int i, x, z;
	for (i = 0; i < list_size(archivosList); i++) {
		t_archivo* archivo = list_get(archivosList, i);
		for (x = 0; x < list_size(archivo->l_bloques); x++) {
			t_bloques* bloque = list_get(archivo->l_bloques, x);

			//copias por cada num_bloque
			for (z = 0; z < list_size(bloque->copias); z++) {
				t_copia* copia = list_get(bloque->copias, z);
				if (copia->id_nodo == nodo->id) {
					copia->nodo_online = 0;
				}

			}

			bool _todas_copias_con_nodos_offline(t_copia* copia) {
				return copia->nodo_online == 0;
			}

			if (!flag) {

				if (list_all_satisfy(bloque->copias,
						(void*) _todas_copias_con_nodos_offline)) {
					if (archivo->estado != NO_DISPONIBLE) {
						archivo->estado = NO_DISPONIBLE;
						flag = true;
						printf(
								"\n El archivo %s se ha desactivado por desconexión de nodo. \n",
								archivo->nombre);
						log_info(loggerMDFS,
								"El archivo %s se ha desactivado por desconexión de nodo. \n",
								archivo->nombre);
					}
				}

			}

		}

		flag = false;
	}
}

void desconexionNodo(int socket_nodo) {
	//busca en nodosListOn por socket_nodo
	t_nodo* nodoEnOn;
	t_nodo* nodoEnOff;

	bool _buscar_por_id_socket(t_nodo* n1) {
		return n1->id_socket == socket_nodo;
	}

	nodoEnOn = list_remove_by_condition(nodosListOn,
			(void*) _buscar_por_id_socket);

	char* mensaje = string_new();
	string_append(&mensaje, "9"); //'9' : informando nodoEnOn desconectado a MaRTA

	if (nodoEnOn != NULL) {
		checkearArchivosDesactivados(nodoEnOn);
		list_add(nodosListOff, nodoEnOn);
		removeNodoDB(nodoEnOn->id, collectionNodoListOn);
		nodoEnOn->id_socket = -1;
		insertarEnDBNodo(nodoEnOn, collectionNodoListOff);

		printf("Desconexion nodoEnOn %d\n", nodoEnOn->id);

		if (socketMarta != -1) {

			string_append(&mensaje, string_itoa(nodoEnOn->id));
			int strlenMensaje = strlen(mensaje);
			sendHeader(SOY_FS, strlenMensaje, socketMarta);
			enviar(socketMarta, mensaje, strlen(mensaje));

		}
	} else {

		nodoEnOff = list_find(nodosListOff, (void*) _buscar_por_id_socket);

		if (nodoEnOff != NULL) {
			nodoEnOff->id_socket = -1;

		}
	}

	if (list_size(nodosListOn) < LISTA_NODOS) {
		estado = NO_DISPONIBLE;
	}

	//marta se desconecto?
	if (socketMarta == socket_nodo) {
		socketMarta = -1;
	}

}

int eliminarNodo() {

	printf("\nLista de nodos para eliminar:\n");
	t_nodo* nodo;
	int i;
	for (i = 0; i < list_size(nodosListOn); i++) {
		nodo = list_get(nodosListOn, i);
		printf("%d - ID: %d IP: %s PUERTO: %s CANTIDAD BLOQUES LIBRES: %d\n",
				i + 1, nodo->id, nodo->ip, nodo->puerto, nodo->bloques_libres);
	}
	if (list_size(nodosListOn) == 0) {
		printf("\n(Vacío)");
	}
	printf("\n0 - Volver \n");
	printf("Seleccione el índice del nodo a eliminar: ");
	int scan;
	scanf("%d", &scan);

	if (scan == 0) {
		return -1;
	} else if (scan > list_size(nodosListOn)) {
		printf("El numero que usted ingreso no es una opcion valida");
		return -1;
	}

	nodo = list_remove(nodosListOn, scan - 1);
	removeNodoDB(nodo->id, collectionNodoListOn);
	list_add(nodosListOff, nodo);
	insertarEnDBNodo(nodo, collectionNodoListOff);

	if(socketMarta != -1){
		//avisa a MaRTA
		char* mensaje = string_new();
		string_append(&mensaje, "9"); //'9' : informando nodo desconectado a MaRTA
		string_append(&mensaje, string_itoa(nodo->id));
		int strlenMensaje = strlen(mensaje);
		sendHeader(SOY_FS, strlenMensaje, socketMarta);
		enviar(socketMarta, mensaje, strlen(mensaje));
	}


	//marca como no disponible a los archivos que tengan bloques que no pueden ser servidos
	checkearArchivosDesactivados(nodo);

	if (list_size(nodosListOn) < LISTA_NODOS) {
		estado = NO_OPERATIVO;
		printf("\nSistema en modo NO OPERATIVO.");
		log_info(loggerMDFS, "Sistema en modo NO OPERATIVO.");
	}

	return 0;
}

void crearFilesystem(){

	archivosList = list_create();
	mdfs = list_create();

	recuperarDirectorio();

	if (list_is_empty(mdfs)) {

		t_elemento *elemento = (t_elemento *) malloc(sizeof(t_elemento));
		elemento->directorio = "/";
		elemento->padre = -1;
		elemento->index = 0;

		if (list_add(mdfs, elemento) == -1) {
			exitError("ERROR: no se pudo iniciar el sistema.");
		}

		insertarDirectorioDB(elemento);
	}

	recuperarArchivos();

}

void copiarArchivoALocal(t_archivo* archivo, char* path_absoluto){

//	string_append(&ubicacion_fs_local,archivo->nombre);

	int acum = 0;

	FILE* file;
	if(!(file = fopen(path_absoluto,"w"))){
		perror("El archivo no se pudo crear.");
	}

	int cant_bloques = getCantidadBloquesArchivo(archivo->size_bytes);

	int i,j;
	for(i=0; i<list_size(archivo->l_bloques);i++){

		t_bloques* bloque = list_get(archivo->l_bloques,i);
		int bloque_copiado = 0;
		j = 0;
		while(bloque_copiado == 0){
			t_copia* copia = list_get(bloque->copias,j);
			//envia solo nodos conectados
			if(copia->nodo_online != 0){
				//SOY_FS bytesDeCopia->bloque
				//recibir de nodo
				char* buffer = string_new();
				string_append(&buffer,GET_BLOQUE);
				string_append(&buffer,string_itoa(copia->bloque));
				string_append(&buffer,",");

				int strLenBuffer = strlen(buffer);

				bool _buscar_por_id(t_nodo* n1){

					return (n1->id == copia->id_nodo);
				}

				t_nodo* nodo = list_find(nodosListOn, (void*) _buscar_por_id);

				sendHeader(SOY_FS,strLenBuffer, nodo->id_socket);
				enviar(nodo->id_socket,buffer,strLenBuffer);

				sem_wait(&mutexFork);
				//	map_bloque y map_bloque_size estan seteados

				int buffer_sendsize = bloque->tamanio_grabado;
				//escribo la data
				fseek(file, acum, SEEK_SET);
				if (fwrite(map_bloque,1,buffer_sendsize,file) == -1){
					printf("\nERROR al escribir en %s",path_absoluto);
					log_info(loggerMDFS,"ERROR al escribir en %s",path_absoluto);
				}
				printf("Acum %i , with buffer %d \n", acum, buffer_sendsize);
				acum += buffer_sendsize;

				bloque_copiado = 1;
				free(map_bloque);
				printf("\nBloque N° %d/%d copiado\n",i+1,cant_bloques);
				log_info(loggerMDFS,"Bloque N° %d/%d copiado\n",i,cant_bloques);
			}
			j++;	//ojo

		}

	}

	fflush(file);
	fclose(file);
}

int exportarArchivo(){

	imprimirArchivosExistentes();

	printf("\n0 - Volver \n");
	printf("Seleccione el índice del archivo a exportar : ");
	int scan;
	scanf("%d",&scan);

	if (scan == 0){
		return -1;
	}

	if(scan-1 > list_size(archivosList)){
		printf("\nERROR: no existe el índice %d .",scan);
		log_info(loggerMDFS,"ERROR: no existe el índice %d .",scan);
		return -1;
	}

	t_archivo* archivo = list_get(archivosList,scan-1);

	if(archivo->estado == NO_DISPONIBLE){
		printf("\nEl archivo seleccionado se encuentra NO DISPONIBLE.");
		log_info(loggerMDFS,"El archivo seleccionado se encuentra NO DISPONIBLE.");
		return -1;
	}

	char* ubicacion_fs_local = (char*) malloc (128*sizeof(char));
	printf("Ubicación del filesystem local :");
	scanf("%s",ubicacion_fs_local);

	char* path_absoluto = string_new();
	string_append(&path_absoluto,ubicacion_fs_local);
	string_append(&path_absoluto, "/"); //lo agrego para que le puedas mandar por ej /home/utnso/Escritorio (sin / al final)
	string_append(&path_absoluto,archivo->nombre);

	copiarArchivoALocal(archivo, path_absoluto);

	log_info(loggerMDFS,"Se ha exportado el archivo.");

	return 0;
}

int generarMD5(){

	imprimirArchivosExistentes();

	printf("\n0 - Volver \n");
	printf("Seleccione el índice del archivo : ");
	int scan;
	scanf("%d",&scan);

	if (scan == 0){
		return -1;
	}

	if(scan-1 > list_size(archivosList)){
		printf("\nERROR: no existe el índice %d .",scan);
		log_info(loggerMDFS,"ERROR: no existe el índice %d .",scan);
		return -1;
	}

	t_archivo* archivo = list_get(archivosList,scan-1);

	if(archivo->estado == NO_DISPONIBLE){
		printf("\nEl archivo seleccionado se encuentra NO DISPONIBLE.");
		log_info(loggerMDFS,"El archivo seleccionado se encuentra NO DISPONIBLE.");
		return -1;
	}

	char* path_absoluto = string_new();
	string_append(&path_absoluto,"./archivoMD5_tmp.txt");

	copiarArchivoALocal(archivo, path_absoluto);

	system("md5sum ./archivoMD5_tmp.txt");
	system("rm ./archivoMD5_tmp.txt");

	return 0;
}

void mostrarEstado() {
	if(estado){printf("\nESTADO: DISPONIBLE"); } else {printf("\nESTADO: NO DISPONIBLE");}

	//calculo tamaño total
	float total = 0;
	float disponible = 0;
	int i;
	for (i=0; i<list_size(nodosListOn); i++) {
		t_nodo* nodo = list_get(nodosListOn,i);
		total += nodo->size_datos_mb;
		disponible += (nodo->bloques_libres * 20);
	}

	float total_gb = total / 1024;
	float disponible_gb = disponible / 1024;

	printf("\nESPACIO TOTAL: %03.2f GB",total_gb);
	printf("\nESPACIO TOTAL DISPONIBLE: %03.2f GB",disponible_gb);
	printf("\nESPACIO DISPONIBLE PARA ARCHIVOS: %03.2f GB",(disponible_gb/3));

}

void consola(){

	while(1){

		printf("\n Consola Filesystem \n \n");

		printf(
				"Comandos:\n"
				"1- Formatear \n"
				"2- Shell \n"
				"3- Importar archivo \n"
				"4- Exportar archivo \n"
				"5- MD5 \n"
				"6- Ver/Borrar/Copiar bloques de archivos \n"
				"7- Agregar nodo \n"
				"8- Eliminar nodo \n"
				"9- Ver estado MDFS \n \n"
		);

		int scan;
		scanf("%d", &scan);

//		printf("\n \n \n \n \n \n \n \n \n \n");
		switch (scan){
			case 1:
				formatear();
				break;
			case 2:
				buscarDirectorios(0);
				break;
			case 3:
				if(estado != NO_OPERATIVO){
					importarArchivo();
				}else{
					printf("\nEste menu esta inhabilitado. El sistema se encuentra en modo NO OPERATIVO");
					log_info(loggerMDFS,"El menú seleccionado esta inhabilitado. El sistema se encuentra en modo NO OPERATIVO");
				}
				break;
			case 4:
//				if(estado != NO_OPERATIVO){
					exportarArchivo();
//				}else{
//					printf("\nEste menu esta inhabilitado. El sistema se encuentra en modo NO OPERATIVO");
//					log_info(loggerMDFS,"El menú seleccionado esta inhabilitado. El sistema se encuentra en modo NO OPERATIVO");
//				}
				break;
			case 5:
//				if(estado != NO_OPERATIVO){
					generarMD5();
//				}else{
//					printf("\nEste menu esta inhabilitado. El sistema se encuentra en modo NO OPERATIVO");
//					log_info(loggerMDFS,"El menú seleccionado esta inhabilitado. El sistema se encuentra en modo NO OPERATIVO");
//				}
				break;
			case 6:
				menuSiete();
				break;
			case 7:
				agregarNodo();
				break;
			case 8:
				eliminarNodo();
				break;
			case 9:
				mostrarEstado();
		}

		char c;
		printf("\nPresione ENTER para continuar...\n");
		scanf ("%c",&c);
		getchar();
	}
}

//FIXME: TESTEAR!!!!
int enviarUbicacionMaRTA(char* ruta){

	printf("Buscando ubicacion %s para marta\n", ruta);
	//cant bloques * bloque1ubicacion1 - bloque1ubicacion2 - bloque1ubicacion3 * bloque2ubicacion1 - bloque2ubicacion2 - bloque2ubicacion3
	char* mensajeAMaRTA = string_new();

	//ej: ruta = '/user/asd/algo/esto.txt'

	//	obtengo esto.txt
	char* name_archivo = string_new();
	name_archivo = obtenerNombreUltimoDirectorio(ruta);

	//	obtengo /user/asd/algo
	char* directorio = string_new();
	directorio = getPathArchivo(ruta);

	//	valido que exista /user/asd/algo
	t_list* caminoDirectorio = armarDir(directorio);
	if(list_is_empty(caminoDirectorio)){
		printf("ERROR SOLICITUD MaRTA: El directorio no existe. \n");
		log_info(loggerMDFS,"ERROR SOLICITUD MaRTA: El directorio no existe. ");
		enviar(socketMarta,"-1",3);
		return -1;
	}


	int size = list_size(caminoDirectorio);
	t_elemento* dir = list_get(caminoDirectorio, size-1);

	//	valido que existe esto.txt
	t_archivo* archivo = existeArchivo(name_archivo, dir->index);
	if (archivo == NULL){
		printf("ERROR SOLICITUD MaRTA: El archivo no existe. \n");
		log_info(loggerMDFS,"ERROR SOLICITUD MaRTA: El archivo no existe. ");
		enviar(socketMarta,"-1",3);
		return -1;
	}

	char* path = string_new();
	//obtengo el path del directorio padre del archivo	(esto.txt)
	path = getPathCompleto(archivo->index_directorio_padre);

	//chequeo si el path ingresado es el mismo q el directorio padre del archivo
	// (si /user/asd/algo es el padre de esto.txt)
	if(!(archivo->index_directorio_padre == dir->index)){
		printf("ERROR SOLICITUD MaRTA: no existe el archivo %s en la ubicación %s",archivo->nombre, path);
		log_info(loggerMDFS,"ERROR SOLICITUD MaRTA: no existe el archivo %s en la ubicación %s",archivo->nombre, path);
		enviar(socketMarta,"-1",3);
		return -1;
	}

	if(archivo->estado == NO_DISPONIBLE){
		printf("ERROR SOLICITUD MaRTA: el archivo %s se encuentra NO DISPONIBLE",archivo->nombre);
		log_info(loggerMDFS,"ERROR SOLICITUD MaRTA: el archivo %s se encuentra NO DISPONIBLE",archivo->nombre);
		enviar(socketMarta,"-1",3);
		return -1;
	}

	int cant_bloques_totales;
	cant_bloques_totales = getCantidadBloquesArchivo(archivo->size_bytes);

	string_append(&mensajeAMaRTA, string_itoa(cant_bloques_totales));
	string_append(&mensajeAMaRTA, "*");

	int i,j;
	for(i=0; i<list_size(archivo->l_bloques);i++){

		t_bloques* bloque = list_get(archivo->l_bloques,i);

		bool _nodos_conectados(t_copia* c){
			return c->nodo_online == 1;
		}

		int cant_copias_nodos_conectados = list_count_satisfying(bloque->copias,(void*) _nodos_conectados);

		if(cant_copias_nodos_conectados > 3){
			//envia solo conectados
			int cant_enviados = 0;
			int j = 0;
			while(cant_enviados != 3){
				t_copia* copia = list_get(bloque->copias,j);
				if (copia->nodo_online == 1){
					string_append(&mensajeAMaRTA, string_itoa(copia->id_nodo));
					string_append(&mensajeAMaRTA, "_");
					string_append(&mensajeAMaRTA, string_itoa(copia->bloque));
					string_append(&mensajeAMaRTA, "-");
					cant_enviados++;
				}
				j++;
			}
		} else {

			int cant_enviados = 0;

			//intenta enviar 3
			for (j=0; j<list_size(bloque->copias) || j<3; j++){
				t_copia* copia = list_get(bloque->copias,j);
				if (copia->nodo_online != 1)continue;
				string_append(&mensajeAMaRTA, string_itoa(copia->id_nodo));
				string_append(&mensajeAMaRTA, "_");
				string_append(&mensajeAMaRTA, string_itoa(copia->bloque));
				string_append(&mensajeAMaRTA, "-");

				cant_enviados++;
			}

			if(cant_enviados < 3){
				int k;
				for(k=0; k<3-cant_enviados; k++){
					string_append(&mensajeAMaRTA, "0");	//nodo desconectado
					string_append(&mensajeAMaRTA, "_");
					string_append(&mensajeAMaRTA, "0");
					string_append(&mensajeAMaRTA, "-");
				}
			}

		}

		mensajeAMaRTA = string_substring_until(mensajeAMaRTA, strlen(mensajeAMaRTA)-1);
		string_append(&mensajeAMaRTA, "*");

	}

	mensajeAMaRTA = string_substring_until(mensajeAMaRTA, strlen(mensajeAMaRTA)-1);

	enviar(socketMarta, mensajeAMaRTA, strlen(mensajeAMaRTA));

	return 0;
}

int reciboBloque(int socket, char* buffer){

	char** array = string_split(buffer, ",");
	map_bloque_size = atoi(array[0]);

	map_bloque = malloc(map_bloque_size);

	int n;
	int s = 0;
	while( (s != map_bloque_size) && ( (n = recv(socket, map_bloque + s, map_bloque_size - s, 0))  > 0 ) ){
	  	s = s + n;
	}
	log_info(loggerMDFS,"Recibo bloque: ");
	log_info(loggerMDFS,"Bytes recibidos: %d / %d \n", s, map_bloque_size);

	//asigno y libero semaforo
	sem_post(&mutexFork);



	return 0;
}

void indexMdfs() {

	if (hayGlobalesMdfs()) {
		recuperarGlobalesMdfs();
	} else {
		index_mdfs = 0;
		insertarEnDBGlobalesMdfs();
	}
}

int main() {

 	t_config* config = config_create(CONFIG_PATH);
	LISTA_NODOS = config_get_int_value(config, "LISTA_NODOS");
	PUERTO_LISTEN = config_get_int_value(config, "PUERTO_LISTEN");

	//map = malloc(20*MB);

	int listener = crearListener(PUERTO_LISTEN);
	t_struct_select* parametros = inicializarSelect(listener, 1024);

	//inicializo listas
	nodosListOn = list_create();
	nodosListOff = list_create();

	//inicializo logger
	loggerMDFS = log_create("mdfs.log", "MDFS", 0, LOG_LEVEL_DEBUG); //Creo el Log de Kernel

	//incializo semaforo
	sem_init(&mutexFork, 0, 0);

	mongoc_init();
	client = mongoc_client_new("mongodb://localhost:27017/");
	collectionArchivo = mongoc_client_get_collection(client,"Filesystem","Filesystem");
	collectionNodoListOn = mongoc_client_get_collection(client,"Filesystem","NodosListOn");
	collectionNodoListOff = mongoc_client_get_collection(client,"Filesystem","NodosListOff");
	collectionGlobales = mongoc_client_get_collection(client,"Filesystem","Globales");
	collectionDirectorio = mongoc_client_get_collection(client,"Filesystem","Directorio");

	indexMdfs();

	crearFilesystem();

	recuperarNodoDB(collectionNodoListOff, nodosListOff);
	recuperarNodoDB(collectionNodoListOn, nodosListOn);

	pthread_t threadConsola;
	pthread_create(&threadConsola, NULL, (void*) consola, NULL);

	while(1){
		int socket = getSocketChanged(parametros);

		if(socket == -1)
			continue;

		char* buffer = string_new();

		string_append(&buffer,parametros->buffer);

		if(parametros->owner == '0')
			switchMarta(socket, buffer);
		if(parametros->owner == '1')
			switchNodo(socket, buffer);
		if(parametros->owner == '2')
			reciboBloque(socket, buffer);

		free (parametros->buffer);

	}

	//no se si hace falta, nunca deberian cerrarse
	pthread_join(threadConsola, NULL);

	mongoc_collection_destroy(collectionDirectorio);
	mongoc_collection_destroy(collectionArchivo);
	mongoc_collection_destroy(collectionNodoListOn);
	mongoc_collection_destroy(collectionNodoListOff);
	mongoc_collection_destroy(collectionGlobales);
	mongoc_client_destroy(client);

	return EXIT_SUCCESS;
}
