#define _USE_LARGEFILE64
#define _FILE_OFFSET_BITS 64

#include "Nodo.h"

typedef struct{
	int* numero;
	char* nombre;
} t_nodo_list ;

typedef struct{
	char* nombre;
} t_archivos_list ;

t_log* loggerNodo;


int pedirArchivoANodo(char* ip, char* puerto, char* fileName){

	printf("Pidiendo archivo %s a %s:%s \n",fileName, ip, puerto);
	int socketnodo = conectar(ip, puerto);

	char* msj = string_new();
	string_append(&msj, "0"); //getfile
	string_append(&msj, fileName);

	sendHeader(NODO, string_length(msj), socketnodo);
	enviar(socketnodo, msj, string_length(msj));

	char* bufferHeader = calloc(HEADERSIZE + 1, 1);
	recibir(socketnodo, bufferHeader, HEADERSIZE);

	char* msj1 = string_substring_from(bufferHeader, 1); //cantidad de bytes del tamanio

	char* mensaje = calloc(atoi(msj1), 1);
	recibir(socketnodo, mensaje, atoi(msj1)); //tamanio

	int length = atoi(mensaje);

	void* buffer = malloc(length);

	int n;
	int sizeRecv = 0;

	while( (sizeRecv != length) && ( (n = recv(socketnodo, buffer + sizeRecv, length - sizeRecv, 0))  > 0 ) ){
	  	sizeRecv = sizeRecv + n;
	}
	printf("Archivo recibido.\n");

	char* nombre = string_new();
	string_append(&nombre, DIR_TEMP);
	string_append(&nombre, "/");
	string_append(&nombre, fileName);

	FILE* newFile;
	if((newFile = fopen(nombre, "w+")) == NULL) perror("Error creando archivo recibido");
	if(fwrite(buffer, length, 1, newFile) != 1) perror("Error escribiendo en el archivo");

	fflush(newFile);
	fclose(newFile);

	return 0;
}

void* getBloque(int id){

	void* map;
	int offset = id * BLOCKSIZE;

	map = map_data + offset;

	return map;

}

void armarListaArchivos(char* listString, t_list* list){

	t_list* temp = list_create();

	char** arrayNodosString = string_split(listString, "-");

	int i;
	for(i=0; ; i++){

		if(arrayNodosString[i] == NULL)
			break;

		t_archivos_list* archivos = malloc(sizeof(t_archivos_list));
		archivos->nombre = string_new();

		string_append(&(archivos->nombre), arrayNodosString[i]);

		list_add(temp, archivos);

	}

	*list = *temp;

};

int cargarVariablesReducer(char* buffer, t_list* listaArchivos, int** jobSocket, int socket){

	char** array = string_split(buffer, ",");

	armarListaArchivos(array[0], listaArchivos);
	**jobSocket = socket;

	return atoi(array[1]);

}

int tamanioArray(char** tabla)
{
	int tam=0;
	while (tabla[tam]!='\0')
		tam++;
	return tam;
}

//juro que no la copie del fs
char* obtenerNombreArchivo(char* path){
	char* nombre;
	nombre = (char*) malloc (128*sizeof(char));
	char** directorios;
	int b;
	directorios = string_split(path,"/");
	b = tamanioArray(directorios);
	nombre = directorios[b-1];
	return nombre;
}

int cargarVariablesMapper(char* buffer, int** idBlock, char** tempFilename, int** jobSocket, int socket){

	char** array = string_split(buffer, ",");

	**idBlock = atoi(array[0]);
	*tempFilename = array[1];
	**jobSocket = socket;

	return atoi(array[2]);

}


void map(mapThreadParams* t){

	bool falla = false;

	log_info(loggerNodo, "Solicitud para hacer un Map con el nodo en estado: %s", NODO_NUEVO);

	void* codigoMapper = t->codigoMapper;
	int idBlock;
	char* tempFilename = string_new();
	int socket;

//	string_append(&codigoMapper, t->codigoMapper);
	string_append(&tempFilename, t->tempFilename);
	idBlock = *(t->idBlock);
	socket = *(t->jobSocket);

	char* templateName = string_new();
	string_append(&templateName, DIR_TEMP);
	string_append(&templateName, tempFilename);

	int temp_des = mkstemp(templateName);  //en templatename queda guardado el nombre creado
	close(temp_des);

	//guardo el codigo del mapper
	char* rutaMapper = string_new();
	string_append(&rutaMapper, DIR_TEMP);
	string_append(&rutaMapper, "/mapper");
	string_append(&rutaMapper, string_itoa(*(t->idBlock)));

//	unlink(rutaMapper);

	int fdmapper = creat(rutaMapper, S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH);

	int s = 0;
	int n = 0;

	while( (s!= *(t->codeLength)) && ((n = write(fdmapper, codigoMapper + s, *(t->codeLength) - s)) > 0)){
		s = s + n;
	}

	close(fdmapper);

//	FILE* mapper = fopen(rutaMapper, "wb");
//	if(mapper == NULL) perror("archivo mapper");
//
//	if(fwrite (codigoMapper,*(t->codeLength),1 ,mapper) != 1) perror("fwrite map");
//
//	fflush(mapper);
//	fclose(mapper);

	sleep(10);

	printf("Preparando map bloque %d\n", *(t->idBlock));

	pthread_mutex_lock(&mutexFork);

	char* comando = string_from_format("%s | sort > %s", rutaMapper, templateName);

	FILE* pipe;
	if ((pipe = popen(comando, "w")) == NULL) perror("popen");

    pthread_mutex_unlock(&mutexFork);

     //le pasamos los datos
     char* buffer = getBloque(idBlock);
     char* pointerZero = memchr(buffer, '\0', BLOCKSIZE);
     int bytesToWrite = pointerZero != NULL ? pointerZero - buffer : BLOCKSIZE;

     int pipeDes = fileno(pipe);
     printf("Mapeando bloque %d\n", *(t->idBlock));

     write(pipeDes, buffer, bytesToWrite);
     //fwrite(buffer, 1, bytesToWrite, pipe);

     int ret = pclose(pipe);

     if(ret != 0) falla = true;

  	 if(unlink(rutaMapper) != 0) perror("unlink");

  	 printf("Fin map bloque %d, ", *(t->idBlock));

     falla ? printf("con errores\n") : printf("sin errores\n");

  	 //0 si no fallo
  	 char* response = string_new();

  	 string_append(&response, string_itoa(falla));

  	 enviar(socket, response, strlen(response));

}

//TODO testear cambio
void reduce(reduceThreadParams* t){

	bool falla = false;

	puts("Iniciando reduce");
	log_info(loggerNodo, "Solicitud para hacer un Reduce con el nodo en estado: %s", NODO_NUEVO);

	void* codigoReducer = t->codigoReducer;
	t_list* listaArchivos = t->fileList;
	char* tempFilename = string_new();
	int socket = *(t->jobSocket);;

	//guardo el codigo del reducer
	char* rutaReduce = string_new();
	string_append(&rutaReduce, DIR_TEMP);
	string_append(&rutaReduce, "/reducer");

	int fdreducer = creat(rutaReduce, S_IRWXU | S_IRGRP | S_IROTH | S_IXGRP | S_IXOTH);

	int s = 0;
	int n = 0;

	while( (s!= *(t->codeLength)) && ((n = write(fdreducer, codigoReducer + s, *(t->codeLength) - s)) > 0)){
		s = s + n;
	}

	close(fdreducer);

//	FILE* reducer = fopen(rutaReduce, "wb");
//	if(reducer == NULL) perror("archivo reducer");
//
//	if(fwrite (codigoReducer,*(t->codeLength),1 ,reducer) != 1) perror("fwrite reduce");
//
//	fflush(reducer);
//	fclose(reducer);

	sleep(10);

//	chmod(rutaReduce, 0777);

	char* inputsPath = string_new();

	//arma un string con los nombres de cada archivo en la lista separados por un espacio
    void _appendNameToString(t_archivos_list *n) {

//    	printf("[DEBUG] agregando a merge archivo %s\n", n->nombre);

    	string_append(&inputsPath, DIR_TEMP);  // /home/.../tmp
    	string_append(&inputsPath, n->nombre); // /mapbx
    	string_append(&inputsPath, " ");
    }

    list_iterate(listaArchivos, (void*) _appendNameToString);

    //le saca el ultimo espacio
    inputsPath = string_substring_until(inputsPath, strlen(inputsPath)-1);

    //arma el comando para mergear
    char* mergeBash = string_new();
    char* outputPath = string_new();

	string_append(&outputPath, DIR_TEMP);
	string_append(&outputPath, "/sorttempXXXXXX");

	int temp_des = mkstemp(outputPath);

    string_append(&mergeBash, "sort -s -m -o ");
    string_append(&mergeBash, outputPath);
    string_append(&mergeBash, " ");
    string_append(&mergeBash, inputsPath);

    pid_t pid = fork();

	if (pid == 0){
		puts("Ejecutando merge");
		execl("/bin/sh", "sh", "-c", mergeBash, NULL);
	}
 	 //esperamos que termine de hacer el sort
    int statval;
//    wait(&statval);
    waitpid(pid, &statval, 0);
    if(WIFEXITED(statval)){
    	//si salio con algun return
    	puts("Merge termino bien");
    }
    else{
    	//si rompio en algun lado
    	falla = true;
    	error_show("Merge fallo\n");
    }

	char* temp = string_new();
	string_append(&temp, DIR_TEMP);
	string_append(&temp, tempFilename);


    char* redOutputPath = string_new();

	string_append(&redOutputPath, DIR_TEMP);
	string_append(&redOutputPath, "/reduceXXXXXX");

	int outputFileno = mkstemp(redOutputPath);

	pthread_mutex_lock(&mutexFork);

	char* comando = string_from_format("%s > %s", rutaReduce, redOutputPath);
//	printf("[DEBUG]comando para el pipe = %s\n", comando);

	FILE* pipe;
	if ((pipe = popen(comando, "w")) == NULL) perror("popen reduce");

	pthread_mutex_unlock(&mutexFork);

	FILE* redINPUT  = fdopen(temp_des, "r+");

	char buffer[512];
	int nread;

	int pipeDes = fileno(pipe);

//	printf("[DEBUG] escribiendo en el pipe \n");
    while ((nread = fread(buffer, 1, sizeof buffer, redINPUT)) > 0)
        write(pipeDes, buffer, nread);

	fclose(redINPUT);
	int ret = pclose(pipe);
//	printf("[DEBUG] return del pipe %d\n", ret);
	if(ret != 0) falla = true;

    //borra el temporal y el reducer
// 	 unlink(outputPath);
// 	 unlink(rutaReduce);
//
// 	 //borra todos los map
//     void _delete_temp(t_archivos_list *n) {
//
//    	char* rutamapdelete = string_new();
//     	string_append(&rutamapdelete, DIR_TEMP);  // /home/.../tmp
//     	string_append(&rutamapdelete, n->nombre); // /mapbx
//
//     	if(unlink(rutamapdelete) == -1) perror("unlink limpiar temporales");
//     }
//
//     list_iterate(listaArchivos, (void*) _delete_temp);

     printf("Fin reduce ");
     falla ? printf("con errores\n") : printf("sin errores\n");

  	 char* resp = string_new();

     string_append(&resp, ID);
  	 string_append(&resp, ",");
  	 string_append(&resp, string_itoa(falla));
  	 string_append(&resp, ",");
  	 string_append(&resp, obtenerNombreArchivo(redOutputPath));

  	 enviar(socket, resp, strlen(resp));

}

void* getFileContent(char* name, int* size){

	char* map;

	char* filename = string_new();
	filename = string_duplicate(DIR_TEMP);
	string_append(&filename, "/");
	string_append(&filename, name);

	FILE* TEMP = fopen(filename, "r");

	fseek(TEMP, 0, SEEK_END);
	int filesize = ftell(TEMP);
	rewind(TEMP);

	*size = filesize;

	struct stat stats;
	if(stat(filename, &stats) < 0) perror("stat getFileContent");

//	printf("\nSTAT SIZE %jd\nfilesize = %d\n\n", (intmax_t)stats.st_size, filesize);

	if( (map = mmap(NULL, filesize, PROT_READ, MAP_SHARED, fileno(TEMP), 0)) == MAP_FAILED)
		error_show("MAP_FAILED getFileContent name: %s \n", name);

	fflush(TEMP);
	fclose(TEMP); //FIXME no se si esta bien, hay que desmapear esto

	return map;
}

int setBloque(int id, char* data, int size) {

	DATOS = fopen(ARCHIVO_BIN, "rb+");

	if (DATOS) {

		//escribir los datos
		fseek(DATOS, id * BLOCKSIZE, SEEK_SET);
		int success = fwrite(data, size, 1, DATOS);

		if(!success) perror ("Error en setBloque");

		if (size != (20 * MB)) {
			char barracero[1];
			barracero[0] = '\0';
			fwrite(barracero, 1, 1, DATOS);
		}

		fflush(DATOS);
		fclose(DATOS);

		printf("\nGrabando en bloque %d.\n", id);

		return 1;
	} else
		error_show("setBloque error al abrir DATOS.bin \n");

	return -1;
}

void switchJob(int socket, char* buffer){

	switch(buffer[0]){

		case '0':{ //hilo mapper

			mapThreadParams* t = malloc(sizeof(mapThreadParams));

			t->codeLength = malloc(sizeof(int));
			t->idBlock = malloc(sizeof(int));
			t->tempFilename = string_new();
			t->jobSocket = malloc(sizeof(int));

			int codeLength = cargarVariablesMapper (string_substring_from(buffer, 1), &t->idBlock, &t->tempFilename, &t->jobSocket, socket);

			*(t->codeLength) = codeLength;

			void* cod = malloc(codeLength);

			int n = 0;
			int s = 0;
			while( (s != codeLength) && ( (n = recibir(*(t->jobSocket), cod + s, codeLength - s))  > 0 ) ){
			  	s = s + n;
			}

			t->codigoMapper = cod;

			pthread_t mapThread;

			pthread_create(&mapThread, NULL, (void*) map, t);
			pthread_detach(mapThread);

			break;
		}

		case '1':{ //hilo reducerviuvuy

			reduceThreadParams* t = malloc(sizeof(reduceThreadParams));

			t->codeLength = malloc(sizeof(int));
			t->fileList = list_create();
			t->jobSocket = malloc(sizeof(int));

//			printf("[DEBUG] Pedido de reduce\nMensaje: %s\n", string_substring_from(buffer, 1));

			int codeLength = cargarVariablesReducer (string_substring_from(buffer, 1), t->fileList, &t->jobSocket, socket);

//			printf("[DEBUG] Codigo a recibir: %d bytes\n", codeLength);

			*(t->codeLength) = codeLength;

			void* cod = malloc(codeLength);

			int n = 0;
			int s = 0;
			while( (s != codeLength) && ( (n = recibir(*(t->jobSocket), cod + s, codeLength - s))  > 0 ) ){
			  	s = s + n;
			}

			t->codigoReducer = cod;

//			printf("[DEBUG] Creando hilo reduce.\n");

			pthread_t reduceThread;

			pthread_create(&reduceThread, NULL, (void*) reduce, t);
			pthread_detach(reduceThread);

			break;
		}

		case '2':{ //copiar archivos

			char* mensaje = string_substring_from(buffer, 1);

			char** msj = string_split(mensaje, ",");
			char* ip = string_new();
			char* puerto = string_new();
			char* archivos = string_new();

			string_append(&ip, msj[0]);
			string_append(&puerto, msj[1]);
			string_append(&archivos, msj[2]);

			char** arrayArchivos = string_split(archivos, "-");

			int i = 0;
			while(arrayArchivos[i] != NULL){

				int retval = pedirArchivoANodo(ip, puerto, arrayArchivos[i]);

				if(retval != 0){
					perror("Error en pedirArchivoANodo");
				}

				i++;

			}


		}

	}

}

int calcularCantDigitos(int num) {
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


int enviarArchivoAFS(char* fileName){

	printf("FS pide archivo %s \n", fileName);

	int* size = malloc(sizeof(int));
	char* data = getFileContent(fileName, size);

	sendHeader(SOY_NODO, strlen(string_itoa(*size)), socketFS);
	enviar(socketFS, string_itoa(*size), strlen(string_itoa(*size)));
	sendall(socketFS, data, *size);

	printf("Enviando archivo. \n");

	return 0;
}


void switchFS(int socket, char* buffer){

	char* mensaje = string_new();
	string_append(&mensaje, string_substring_from(buffer, 1));

	switch(buffer[0]){

		case '0':{

			char** array = string_split(mensaje, ",");
			int numero_bloque = atoi(array[0]);
			int cant_digitos_sizeToRecv = atoi(array[1]);
			int sizeToRecv =atoi( string_substring_until(array[2],cant_digitos_sizeToRecv) );
//			int sizeToRecv = atoi(array[2]);

			void* buff = malloc(sizeToRecv);

			int n;
			int sizeRecv = 0;

			while( (sizeRecv != sizeToRecv) && ( (n = recv(socket, buff + sizeRecv, sizeToRecv - sizeRecv, 0))  > 0 ) ){
			  	sizeRecv = sizeRecv + n;
			}

			setBloque(numero_bloque, buff, sizeToRecv);

			free(buff);
			break;
		}

		case '1':{//getBloque
			char** array = string_split(mensaje, ",");
			int num_bloque = atoi(array[0]);
			char* data = getBloque(num_bloque);

			char* buffer = string_new();
			string_append(&buffer,string_itoa(BLOCKSIZE));
			string_append(&buffer,",");
			int strlenBuffer = strlen(buffer);

			sendHeader(ENVIO_BLOQUE,strlenBuffer,socketFS);
			enviar(socketFS,buffer,strlenBuffer);
			sendall(socketFS,data,BLOCKSIZE);

			break;
		}

		case '2':{

			char* fileName = string_substring_from(buffer,1);
			enviarArchivoAFS(fileName);

			break;
		}

	}

//	free(buffer);	//libero buffer para no pisar datos
//	buffer = malloc(1024);	//1024 buffersize

}

void switchNodo(int socket, char* buffer){

	switch(buffer[0]){

		//getFileContent - name
		case '0':{

			char* fileName = string_new();
			string_append(&fileName, string_substring_from(buffer, 1));

			printf("Leyendo archivo %s\n", fileName);

			int* size = malloc(sizeof(int));
			char* data = getFileContent(fileName, size);

			sendHeader(NODO, strlen(string_itoa(*size)), socket); //para no hacerlo de nuevo, del otro lado no le doy bola al 1er byte y fue...
			enviar(socket, string_itoa(*size), strlen(string_itoa(*size)));
			sendall(socket, data, *size);

			printf("Enviando archivo. \n");

			/*
			  no se entiende nada:
				le mando 4 bytes, el primero al pedo y los otros 3 con el largo del prox mensaje
				le mando cuantos bytes tiene el archivo
				le mando el archivo
			*/

//			free(data); //con esto rompe, le dejamo el leak ya fue

			break;
		}

	}

}


void conectarAFS(int size_mb){

	char* mensaje = string_new();

	string_append(&mensaje, ID);
	string_append(&mensaje, ",");
	string_append(&mensaje, IP_NODO);
	string_append(&mensaje, ",");
	string_append(&mensaje, PUERTO_NODO);
	string_append(&mensaje, ",");

//	string_append(&mensaje, DATA_FILESIZE);	forma vieja

	string_append(&mensaje, string_itoa(size_mb));
	//


	string_append(&mensaje, ",");

	if(string_equals_ignore_case(NODO_NUEVO,"SI"))
		string_append(&mensaje, CONEXION);
	else
		string_append(&mensaje, RECONEXION);

	int length = strlen(mensaje);

	socketFS = conectar(IP_FS, PUERTO_FS);
	sendHeader(SOY_NODO, length, socketFS);
	enviar(socketFS, mensaje, length);

	log_info(loggerNodo, "Se conecto con el FileSystem con ip:%s y puerto:%s", IP_FS, PUERTO_FS);
}


int main(int argc, char** argv) {

	//truncate -s 1G data.bin

	pthread_mutex_init(&mutexFork, NULL);


	loggerNodo = log_create("Nodo.log", "Nodo", 0, LOG_LEVEL_INFO);

	char* propiedades[8] = {"ID", "PUERTO_FS", "IP_FS", "ARCHIVO_BIN", "DIR_TEMP", "NODO_NUEVO", "IP_NODO", "PUERTO_NODO"};
	char** variables[8] = {&ID, &PUERTO_FS, &IP_FS, &ARCHIVO_BIN, &DIR_TEMP, &NODO_NUEVO, &IP_NODO, &PUERTO_NODO};

	inicializarStrings(variables, 8);

	leerConfig(argv[1], propiedades, variables, 8);


	//abrir archivo de datos
	DATOS = fopen(ARCHIVO_BIN, "rb+");
	if(DATOS == NULL) exitError("No se encontro el archivo de datos.\n");

	int listener = crearListener(atoi(PUERTO_NODO));

	t_struct_select* parametros = inicializarSelect(listener, 1024);

	//forma nueva
	struct stat stats;
	if(stat(ARCHIVO_BIN, &stats) < 0) perror("stat ConectarAFS");
//	printf("\nSTAT SIZE %jd\n", (intmax_t)stats.st_size);
	int size_mb = (intmax_t)stats.st_size / (1024 * 1024);
//	printf("size_mb : %d \n", size_mb);

	conectarAFS(size_mb); //guarda el socket en una variable global

	//map del data.bin completo
	if ((map_data = mmap(NULL, (intmax_t)stats.st_size, PROT_READ, MAP_SHARED, fileno(DATOS),
			0)) == MAP_FAILED) {
		perror("MMAP");
		exitError("MAP_FAILED");
	}

	//agrego el socketFS a la lista de descriptores del select
	FD_SET(socketFS, &parametros->master);
	parametros->temp = parametros->master;
	parametros->maxSock ++;

	while(1){

		int socket = getSocketChanged(parametros);

		if(socket == -1) continue;

		if(parametros->owner == '0')
			switchNodo(socket, parametros->buffer);
		if(parametros->owner == '1')
			switchFS(socket, parametros->buffer);
		if(parametros->owner == '2')
			switchJob(socket, parametros->buffer);

	}

	munmap(map_data,(intmax_t)stats.st_size);

	return EXIT_SUCCESS;
}
