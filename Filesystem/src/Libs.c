#include "Libs.h"

int crearListener (int puerto){

	//config
	int queueMax = 10;
	int optval = 1;

	int listener; // descriptor de socket a la escucha

	struct sockaddr_in socket_cliente;
	socket_cliente.sin_family = AF_INET;
	socket_cliente.sin_addr.s_addr = htons(INADDR_ANY );
	socket_cliente.sin_port = htons(puerto);

	// Crear el socket.

	// AF_INET: Socket de internet IPv4
	// SOCK_STREAM: Orientado a la conexion, TCP
	// 0: Usar protocolo por defecto para AF_INET-SOCK_STREAM: Protocolo TCP/IPv4
	if((listener = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		exitError("Creacion socket listener");

	// Hacer que el SO libere el puerto inmediatamente luego de cerrar el socket.
	setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

	// Vincular el socket con una direccion de red almacenada en 'socket_cliente'.
	if (bind(listener, (struct sockaddr*) &socket_cliente, sizeof(socket_cliente)) != 0)
		exitError("Bind socket listener");

	// Escuchar nuevas conexiones entrantes.
	if (listen(listener, queueMax) != 0)
		exitError("Listen");

	printf("Escuchando conexiones en puerto %d \n", puerto);

	return listener;
}

int conectar (char* ip, char* puerto){

	struct addrinfo hints;
	struct addrinfo *serverInfo;
	int descriptor;

	memset(&hints, 0, sizeof(hints));

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if(getaddrinfo(ip, puerto, &hints, &serverInfo) != 0 )
		exitError("getaddrinfo() \n");

	if ((descriptor = socket(serverInfo->ai_family, serverInfo->ai_socktype, serverInfo->ai_protocol)) < 0)
		exitError("socket() \n");

	if(connect(descriptor, serverInfo->ai_addr, serverInfo->ai_addrlen) < 0 )
		exitError("connect() \n");

	freeaddrinfo(serverInfo);

	return descriptor;
};


int enviar (int descriptor, char* data, int datalength){

	int bytecount;

	if ((bytecount = send(descriptor, data, datalength, 0)) == -1){
		error_show("Send bytecount = -1 \n");
	}

	return (bytecount);
}


int recibir (int descriptor, char* buffer, int datalength) {

	int bytecount;

	memset(buffer, 0, datalength);

	bytecount = recv(descriptor, buffer, datalength, 0);

	return (bytecount);
}

//int recvAll(int descriptor,char* buff,int bytesARecibir){
//	int n;
//	int s = 0;
//	int size = bytesARecibir;
//	while( (s != size) && ( (n = recv(descriptor, buff, size - s, 0))  > 0 ) ){
//	  	s = s + n;
//	   	printf("Bytes recibidos: %d / %d \n", s, size);
//	}
//
//	return s;
//}


int contarDigitos (int num) {

	int count = 1;

	if(num < 0){
		count++;
		num = num * -1;
	}

	while (num / 10 > 0) {
		num = num / 10;
		count++;
	}

	return count;
}


void inicializarStrings(char** arrayVars[], int cantVars){
	int i = 0;
	for(i = 0; i < cantVars; i++){
		*arrayVars[i] = string_new();
	}
}

void leerConfig(char* path, char* properties[], char** vars[], int cantProperties) {

	t_config* config = config_create(path);

	if(config->properties->table_current_size !=0){

		int i;

		for (i = 0; cantProperties > i; i++){
			if(config_has_property(config, properties[i])){

				string_append(vars[i],config_get_string_value(config, properties[i]));

			} else error_show("No se pudo leer el parametro %s \n", properties[i]);
		}

	} else {
		exitError("No se pudo abrir el archivo de configuracion \n");
	}

	if(config != NULL)
			free(config);

}

void exitError(char* error){
	error_show(error);
	exit(1);
}


t_struct_select* inicializarSelect(int listener, int buffersize){

	t_struct_select* params = malloc(sizeof(t_struct_select));

	params->listener = listener;

	params->bufferHeader = malloc(HEADERSIZE);

	FD_ZERO(&params->master);
	FD_ZERO(&params->temp);

	params->maxSock = listener;

	FD_SET(listener, &params->master);

	params->temp = params->master;

	return params;
}


int getSocketChanged(t_struct_select* params) {

	params->temp = params->master;

	//--Multiplexa conexiones
	if (select(params->maxSock + 1, &params->temp, NULL, NULL, NULL ) == -1)
		exitError("Select");

	//--Cicla las conexiones para ver cual cambió
	int i;
	for (i = 0; i <= params->maxSock; i++) {

		//--Si el i° socket no cambió
		if (!FD_ISSET(i, &params->temp))
			continue;

		//--Si el que cambió es el listener
		if (i == params->listener) {

			//--Gestiona nueva conexión
			int socketNuevaConexion;
			if((socketNuevaConexion = accept(params->listener, NULL, 0)) == -1)
				exitError("Accept");
			else {
				//--Agrega el nuevo listener
				printf("Socket accept %d\n", socketNuevaConexion);
				FD_SET(socketNuevaConexion, &params->master);

				if (socketNuevaConexion > params->maxSock)
					params->maxSock = socketNuevaConexion;

				//printf("Nueva conexion, socket numero %d \n", socketNuevaConexion);
			}

		} else {

			//--Gestiona un cliente ya conectado

			int nBytes;
			if ((nBytes = recvHeader(i, params)) <= 0) {
				//--Si cerró la conexión o hubo error
				if (nBytes != 0)
//					printf("Fin de conexion del socket %d. \n", i);
//				else
					error_show("Recv: %s", strerror(errno));
				//--Cierra la conexión y lo saca de la lista
				desconexionNodo(i);
				close(i);
				FD_CLR(i, &params->master);
			} else {
				params->buffer = calloc(nBytes + 1, 1);
				recibir(i, params->buffer, nBytes);
				printf("BUFFER %s\n", params->buffer);
				return i;
			}
		}
	}

	return -1;
}


int CharToInt(char x) {
	int numero = 0;
	char * aux = string_new();
	string_append_with_format(&aux, "%c", x);
	//char* aux = malloc(1 * sizeof(char));
	//sprintf(aux, "%c", x);
	numero = strtol(aux, (char **) NULL, 10);

	if (aux != NULL )
		free(aux);
	return numero;
}

void sendall( int descriptorSocket, const char* buffer, const unsigned int bytesPorEnviar){
	int retorno;
	int bytesEnviados = 0;

//	int strlenBuffer = strlen(buffer);


/*	if(strlenBuffer != (int)bytesPorEnviar)
		printf("sendall: No coinciden el largo del buffer con la cantidad de bytes a enviar. strlen(buffer) = %d, bytesPorEnviar = %d\n", strlenBuffer, (int)bytesPorEnviar);
*/

	while (bytesEnviados < (int)bytesPorEnviar) {
	   retorno = send(descriptorSocket, (char*)(buffer+bytesEnviados), bytesPorEnviar-bytesEnviados, 0);
	   //printf("Bytes enviados: %d / %d \n", retorno, bytesPorEnviar);

	   //Controlo Errores
	   if( retorno <= 0 ) {
		  printf("Error al enviar Datos (se corto el Paquete Enviado), solo se enviaron %d bytes de los %d bytes totales por enviar\n", bytesEnviados, (int)bytesPorEnviar);
//		  perror("El Error Detectado es: ");
		  bytesEnviados = retorno;
		  break;
	   }
	   //Si no hay problemas, sigo acumulando bytesEnviados
	   bytesEnviados += retorno;
	}
}

char* intTo4BytesString (int n){

	char* string = string_new();

	if(n <= 99 && n > 9){
		string_append(&string, "0");
	}else{
		if(n <= 9) string_append(&string, "00");
	}

	string_append(&string, string_itoa(n));

	return string;
}

int sendHeader(char* quienSoy, int length, int socket){

	char* header = string_new();

	string_append(&header, quienSoy);
	string_append(&header, intTo4BytesString(length));

	if(strlen(header) != 4) error_show("La cagaste en el header capo");

	return enviar(socket, header, HEADERSIZE);

}

int recvHeader(int socket, t_struct_select* params){

	int ret = recibir(socket, params->bufferHeader, HEADERSIZE);

	if(ret <= 0) return ret;

	char* msg = string_new();
	msg = string_substring_until(params->bufferHeader, 4);

	params->owner = msg[0];
	int length = atoi(string_substring_from(msg, 1));

	return length;

}

int calcularCantDigitos(int num) {
	//patch v2.32, ahora soporta numeros negativos! llame ya
	int contador = 1;

	if(num < 0){
		contador++;
		num = num * -1;
	}
	while (num / 10 > 0) {
		num = num / 10;
		contador++;
	}

	return contador;
}
