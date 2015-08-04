/*
 * Libs.h
 *
 *  Created on: 27/2/2015
 *      Author: utnso
 */

#ifndef SRC_LIBS_H_
#define SRC_LIBS_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include <resolv.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include "Filesystem.h"
#include <commons/log.h>
#include <commons/error.h>
#include <commons/string.h>
#include <commons/config.h>
 #include <sys/mman.h>
 #include <sys/stat.h>
#include <mongoc.h>

#include "Filesystem.h"

#define HEADERSIZE 4

typedef struct{
	int id;
	char* ip;
	char* puerto;
	//ver
	int id_socket;
	int cant_bloques;
	int bloques_libres;
	int size_datos_mb;
	int* bloques;
	int nodo_nuevo;
} t_nodo ;

typedef struct{
	int id_nodo;
	int nodo_socket;
	int bloque;
	int nodo_online;
}t_copia;

typedef struct{
	int id_bloque;
	t_list* copias;
	int tamanio_grabado;
}t_bloques;

typedef struct{
	char* nombre;
	int size_bytes;
	int index_directorio_padre;
	int estado;
	t_list* l_bloques;
}t_archivo;

typedef struct ElementoLista{
//	t_elemento_datos *datos;
	char *directorio;
	int index;
	int padre;
//	struct ElementoLista *siguiente;

}t_elemento;

//typedef struct {
//  t_elemento *inicio;
//  t_elemento *fin;
//  int tamanio;
//}t_filesystem;
//
//t_filesystem *mdfs;

typedef struct{
	int* numero;
	char* nombre;
} t_nodo_list ;


typedef struct{

	fd_set master;
	fd_set temp;
	char owner;
	int listener;
	int maxSock;
	char* buffer;
	int buffersize;
	char* bufferHeader;

}t_struct_select;

/*
 * crearListener (puerto)
 * Crea un socket listener (hay que hacer el accept de las conexiones)
 * Devuelve el descriptor del socket creado
 */
int crearListener (int);

/*
 * conectar (ip, puerto)
 * Crea un socket y lo conecta a un listener
 * Devuelve el descriptor del socket creado
 */
int conectar (char*, char*);

/*
 * enviar (descriptor, data, datalength)
 * Manda data
 * Devuelve bytecount del send
 */
int enviar (int, char*, int);

/*
 * recibir (descriptor, buffer, datalength)
 * Recibe los datos en el buffer
 * Devuelve bytecount del recv
 */
int recibir (int, char*, int);

/*
 * contarDigitos (numero)
 * Devuelve la cantidad de digitos del numero
 * Devuelve la cantidad de digitos + 1 si es negativo
 */
int contarDigitos (int);

/*
 *leerConfig(path, propiedades, variables, cantidadDePropiedades)
 *Lee el valor de las propiedades
 *del cfg ubicado en el path
 *y lo asigna en las variables
 *
 *Ej: leerConfig(/utnso/asd, {"ip","puerto"}, {&ip, &puerto}, 2);
 */
void leerConfig(char*, char* [], char** [], int);

/*
 *inicializarStrings(arrayDePunterosAStrings, cantidadDeElementosEnElArray)
 *Inicializa todos los strings con string_new()
 */
void inicializarStrings(char** [], int);

/*
 *exitError(error)
 *Imprime el error y termina el proceso
 */
void exitError(char*);

/*
 *inicializarSelect(listener, buffersize)
 *Inicializa datos para usar la funcion getSockChanged
 */
t_struct_select* inicializarSelect(int, int);

/*
 *getSockChanged(structParametros)
 *Recibe puntero al struct de inicializarSelect.
 *Devuelve el socket que cambio y guarda el envio en structParametros.buffer
 */
int getSocketChanged(t_struct_select*);

int CharToInt(char);

int index_mdfs;

t_list* mdfs;

t_elemento* l_mdfs_get_last();

int ins_fin_lista (t_elemento *);

int ins_en_lista_vacia (t_elemento *);

void renombrar(char* , char*, char*, t_elemento*);

void desconexionNodo(int);

void reconectarNodo(int);

void switchNodo(int , char*);

void sendall( int , const char* , const unsigned int );

void crearFilesystem();

int getCantidadBloques(int);

void mongoUpdateArchivo(char* , char* , char* , char* , int );

void insertarEnDBArchivo();

void switchMarta(int , char*);

char* getFileName(char* );

char* string_reverse(char* );

void insertarEnDBGlobalesNodo();

void updateGloablesNodo(int );

void insertarEnDBGlobalesMdfs();

void updateGloablesMdfs();

void removeDBGlobalesNodo();

void removeDBGlobalesMdf();

void removeDBArchivo(t_archivo* );

void insertarEnDBNodo(t_nodo* , mongoc_collection_t *);

void updateEstadoBloque(int , int , int , mongoc_collection_t *);

void updatePuertoNodo(int , char* , mongoc_collection_t *);

void updateIdNodo(int , int , mongoc_collection_t *);

void updateIpNodo(int , char* , mongoc_collection_t *);

void updateSocketNodo(int , int, mongoc_collection_t * );

void updateEstadoNodo(int , int, mongoc_collection_t * );

void updateCantidadBloquesLibres(int , int, mongoc_collection_t *  );

void removeNodoDB(int , mongoc_collection_t* );

void recuperarNodoDB();

void updateArchivoNombre(char* , char* , int );

void updateArchivoPadre(char* , int , int );

void updateArchivoEstado(char* , int , int );

void updateArchivoIdNodo(t_archivo*, int , int , int );

void updateArchivoSocketNodo(char* , int , int , int , int );

void actualizarCantidadDeCopiasDB(t_archivo* );

void updateArchivoBloqueNodo(t_archivo* , int , int , int );

int buscarCopiaPorIdNodo(t_list* , int );

t_bloques* buscarBloquePorIdBloque(t_list* , int );

void updateArchivoEstadoNodo(t_archivo* , int , int , int );

void insertarDirectorioDB(t_elemento* );

void updateNombreDirectorio(t_elemento* , char* );

void updatePadreDirectorio(t_elemento* , int );

void recuperarDirectorio();

void removeDirectorio(t_elemento* );

void eliminarCollectionDirectorio();

void eliminarCollectionNodoOn();

void eliminarCollectionNodoOff();

void eliminarCollectionArchivo();

void eliminarCollectionVariablesGlobales();

int* inicializarArrayDeBloques(int );

t_list* buscarImprimirHijos(t_elemento* , bool );

void imprimirDirectoriosExistentes();

t_elemento* existeDirectorio(char* );

int crearDirectorio();

void eliminarElementoMDFS(t_elemento* );

void eliminarLista(t_list* );

int eliminarDirectorio();

void actualizarRenombreHijos(t_list* , char* , char *);

int tamanioArray(char** );

char* obtenerNombreUltimoDirectorio(char* );

int renombrarDirectorio();

int moverDirectorio();

char* getDirectorioPorIndex(int );

void imprimirArchivosExistentes();

void renombrarArchivo(char* , int );

void eliminarArchivo(int , char*);

int moverArchivo(char* , int );

t_archivo* buscarArchivo(char* , int);

int menuDos();

int menuTres();

int formatear();

void ordenarNodosList();

int getPrimerBloqueLibre(t_nodo* );

int enviarDatosANodos(char* ,t_archivo* ,int, int );

int nodosDisponibles(int );

void destroyNodos(t_list* );

int getCantidadBloquesArchivo(int );

t_archivo* getArchivo(char* );

char* getPathArchivo(char* );

void imprimirBloques(t_archivo* );

int borrarBloques();

int verBloques();

int menuSiete();

int importarArchivo();

int agregarNodo();

void checkearArchivosDesactivados(t_nodo* );

int eliminarNodo();

void consola();

int enviarUbicacionMaRTA(char* );

int exportarArchivo();

char* intTo4BytesString (int);

int sendHeader(char*, int, int);

int recvHeader(int, t_struct_select*);

int reciboBloque(int , char* );

int buscarDirectorios(int );

void recorrerDirectorio(t_list* , char* , int );

t_elemento* getDirectorio(int );

t_elemento* existeDirPorNombre(char* , int );

int cdDir(int , char* );

int renameDir(char* , int );

int moverDir(int , char* );

int buscarDirectorios(int );

int listarDirs();

void listarHijos(int , char*);

void dirsDondeEstas(int );

t_list* armarDir(char* );

int crearDir(int , char* );

void removeDir(t_elemento* );

void removeArchivoPorPadreDir(int );

int rmvDirs(int , char* );

void rmvHijos(int );

char* getPathCompleto(int );

void help();

int existeNombreArchivo(char* , int );

int calcularCantDigitos(int );

#endif /* SRC_LIBS_H_ */
