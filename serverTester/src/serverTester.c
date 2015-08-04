#include "Libs.h"

void probarMapNodo(){

	int socket = conectar("127.0.0.1", "6000");

//	char* mensaje = string_new();
//	string_append(&mensaje, "21/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/count.sh,1_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/1.txt*2_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/2.txt,asdout.txt");
//
//	if( enviar(socket, mensaje, strlen(mensaje)) != -1)
//		printf("MENSAJE ENVIADO: 21/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/count.sh,1_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/1.txt*2_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/2.txt,asdout.txt \n \n ---------------------------------------------------- \n \n");

	char* mensaje2 = string_new();
	string_append(&mensaje2, "20/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/count2.sh,1,/asd");

	if( enviar(socket, mensaje2, strlen(mensaje2)) != -1)
		printf("MENSAJE ENVIADO: 20/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/count.sh,1,/asd \n \n ---------------------------------------------------- \n \n");

}

void probarReduceNodo(){

	int socket = conectar("127.0.0.1", "6000");

	char* mensaje = string_new();
	string_append(&mensaje, "21/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/red.sh,1_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/asd*2_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/asdpruebaparamerge,/reduceout");

	if( enviar(socket, mensaje, strlen(mensaje)) != -1)
		printf("MENSAJE ENVIADO: 21/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/red.sh,1_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/asd*2_/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/tmp/asdpruebaparamerge,/reduceout \n \n ---------------------------------------------------- \n \n");

//	char* mensaje2 = string_new();
//	string_append(&mensaje2, "20/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/count2.sh,1,/asd");
//
//	if( enviar(socket, mensaje2, strlen(mensaje2)) != -1)
//		printf("MENSAJE ENVIADO: 20/home/utnso/git/tp-2015-1c-souvenir-jackitos/Nodo/count.sh,1,/asd \n \n ---------------------------------------------------- \n \n");

}

int main(void) {

	int opcion;

	printf("\n \n1)Probar map nodo \n2)Probar reduce nodo \n3)Libre\n");
	scanf("%d", &opcion);

	if(opcion == 1)
		probarMapNodo();

	if(opcion == 2)
		probarReduceNodo();

	if(opcion == 3){

		printf("\n \n ---------------------------------------------------- \n \n");

		char* ip = string_new();
		char* puerto = string_new();
		char* mensaje = string_new();

		printf("INGRESAR IP \n");
		scanf("%s", ip);

		printf("INGRESAR PUERTO \n");
		scanf("%s", puerto);

		int socket = conectar(ip, puerto);

		printf("INGRESAR MENSAJE \n");
		scanf("%s", mensaje);

		if( enviar(socket, mensaje, strlen(mensaje)) != -1)
			printf("MENSAJE ENVIADO \n \n ---------------------------------------------------- \n \n");

	}

	return EXIT_SUCCESS;
}
