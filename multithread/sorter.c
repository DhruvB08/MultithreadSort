#define _GNU_SOURCE

#include <semaphore.h>
#include <pthread.h>
#include <stddef.h>
#include "mergesort.c"


int main(int argc, char** argv) {
	char readDirName[1000];
	char outputDirName[1000];
	char sortColumn[1000];
	
	//program called using -c flag only
	//Cannot be just -d or -o
	if (argc == 3) {
		if (strcmp(argv[1], "-c") != 0) {
			printf("Invalid arguments to sort\n");
			return 0;
		}
		strcpy(sortColumn, argv[2]);
		getcwd(readDirName, sizeof(readDirName));
		getcwd(outputDirName, sizeof(outputDirName));
	}

	//program called using 2 flags:
	// -c -d, -c -o, -d -c, -o -c
	else if (argc == 5) {
		if(strcmp(argv[1], "-c") ==0){ // 1st flag "-c"
			if(strcmp(argv[3], "-d") ==0){
				strcpy(sortColumn, argv[2]);
				strcpy(readDirName, argv[4]);
				getcwd(outputDirName, sizeof(outputDirName));
				//printf("%s\n", readDirName)
			}else if(strcmp(argv[3], "-o") ==0){
				strcpy(sortColumn, argv[2]);
				strcpy(outputDirName, argv[4]);
				getcwd(readDirName, sizeof(readDirName));
				//printf("%s\n", readDirName)
			}else{
			printf("Invalid arguments to sort\n");
			return 0;
			}
		}else if(strcmp(argv[3], "-c") ==0){ // 2nd flag "-c"
			if(strcmp(argv[1], "-d") ==0){
				strcpy(readDirName, argv[2]);
				strcpy(sortColumn, argv[4]);
				getcwd(outputDirName, sizeof(outputDirName));
				//printf("%s\n", readDirName)
			}else if(strcmp(argv[1], "-o") ==0){
				strcpy(outputDirName, argv[2]);
				strcpy(sortColumn, argv[4]);
				getcwd(readDirName, sizeof(readDirName));
				//printf("%s\n", readDirName)
			}else{
			printf("Invalid arguments to sort\n");
			return 0;
			}
		}else{
			printf("Invalid arguments to sort\n");
			return 0;
		}
	}

	// program called using 3 flags
	// -c -d -o, -c -o -d
	// -d -o -c, -d -c -o
	// -o -d -c, -o -c -d
	else if (argc == 7) {
		if(strcmp(argv[1], "-c") ==0){ // 1st flag "-c"
			if(strcmp(argv[3], "-d") ==0 && strcmp(argv[5], "-o") ==0){
				strcpy(sortColumn, argv[2]);
				strcpy(readDirName, argv[4]);
				strcpy(outputDirName, argv[6]);
				//printf("%s\n", readDirName)
			}else if(strcmp(argv[3], "-o") ==0 && strcmp(argv[5], "-d") ==0){
				strcpy(sortColumn, argv[2]);
				strcpy(outputDirName, argv[4]);
				strcpy(readDirName, argv[6]);
				//printf("%s\n", readDirName)
			}else{
			printf("Invalid arguments to sort\n");
			return 0;
			}
		}else if(strcmp(argv[1], "-d") ==0){ // 1st flag "-d"
			if(strcmp(argv[3], "-o") ==0 && strcmp(argv[5], "-c") ==0){
				strcpy(readDirName, argv[2]);
				strcpy(outputDirName, argv[4]);
				strcpy(sortColumn, argv[6]);
				//printf("%s\n", readDirName)
			}else if(strcmp(argv[3], "-c") ==0 && strcmp(argv[5], "-o") ==0){
				strcpy(readDirName, argv[2]);
				strcpy(sortColumn, argv[4]);
				strcpy(outputDirName, argv[6]);
				//printf("%s\n", readDirName)
			}else{
			printf("Invalid arguments to sort\n");
			return 0;
			}
		}else if(strcmp(argv[1], "-o") ==0){ // 1st flag "-o"
			if(strcmp(argv[3], "-d") ==0 && strcmp(argv[5], "-c") ==0){
				strcpy(outputDirName, argv[2]);
				strcpy(readDirName, argv[4]);
				strcpy(sortColumn, argv[6]);
				//printf("%s\n", readDirName)
			}else if(strcmp(argv[3], "-c") ==0 && strcmp(argv[5], "-d") ==0){
				strcpy(outputDirName, argv[2]);
				strcpy(sortColumn, argv[4]);
				strcpy(readDirName, argv[6]);
				//printf("%s\n", readDirName)
			}else{
			printf("Invalid arguments to sort\n");
			return 0;
			}
		}else{
			printf("Invalid arguments to sort\n");
			return 0;
		}
	}

	//program called with unrecognized number of arguments
	else {
		printf("Invalid arguements to sort\n");
		return 0;
	}

	//printf("initializing global\n");
	//initialize globals
	sem_init(&semaphore, 0, 1);
	globalListStart = createRow();
	globalListEnd = globalListStart;
	//printf("done initializing\n");

	struct argstruct *temp = (struct argstruct*)malloc(sizeof(struct argstruct));			
	strcpy(temp->arg1, readDirName);
	strcpy(temp->arg2, sortColumn);
	
	/*char parms[2][1000];
	strcpy(parms[0], readDirName);
	strcpy(parms[1], sortColumn);
	*/
	
	//printf("first gothroughdir about to be called\n");
	goThroughDir((void *) temp);
	//printf("gothroughdir all done in main\n");
	
	char writeFile[99999];
	strcpy(writeFile, outputDirName);
	strcat(writeFile, "/AllFiles-sorted-");
	strcat(writeFile, sortColumn);
	strcat(writeFile, ".csv");

	//printf("read: %s, write: %s\n", filename, writeFile);
	FILE *fp;
	fp = fopen(writeFile, "w+");

	row *currRow;
	currRow = mergesort(globalListStart->next);
	row *prevRow;
	while (currRow != NULL) {
		int i;
		for (i = 0; i < currRow->numCols; i++) {
			fputs(currRow->columns[i], fp);
			fputs(",", fp);
		}

		fputs("\n", fp);
		prevRow = currRow;
		currRow = currRow->next;
		free(prevRow);
	}

	free(globalListStart);
	fclose(fp);

	printf("Initial TID: %d\n", pthread_self());

	printf("TIDS of all child threads: ");
	//for loop to go through TID array

	printf("\nTotal number of threads: \n");
	return 0;
}


void* goThroughCSV(void * params) {
	//increment thread counter
	//add thread id to array
	struct argstruct *temp = params;
	
	char readName[99999];
	strcat(readName, temp->arg1);
	strcat(readName, "/");
	strcat(readName, temp->arg2);
	
	sort(readName, temp->arg3, temp->arg2);
}

void* goThroughDir(void * dirParams) {

	struct argstruct* temp = dirParams;
	
	//char ** temp = (char **) dirParams;

	char readDirName[99999];
	strcpy(readDirName, temp->arg1);

	char sortColumn[99999];
	strcpy(sortColumn, temp->arg2);

	//printf("%s\n", readDirName);
//	printf("%s\n", sortColumn);

	DIR *dir;
	struct dirent *entry;

//	printf("about to open directory\n");
	if ((dir = opendir(readDirName)) != NULL) {
		//increment tid
		//add tid to array

		//printf("reading dir: %s\n", readDirName);

		while ((entry = readdir(dir)) != NULL) {
			char entryName[256];
			strcpy(entryName, entry->d_name);

			//printf("entry: %s\n", entryName);

			//printf("entryName: %s, entry: %s", entryName, entry);
			if (isCSV(entryName)) {
				struct argstruct* csvtemp = (struct argstruct*)malloc(sizeof(struct argstruct));
				
				strcpy(csvtemp->arg1, readDirName);
				strcpy(csvtemp->arg2, entryName);
				strcpy(csvtemp->arg3, sortColumn);
				/*
				char params[3][99999];
				strcpy(params[0], readDirName);
				strcpy(params[1], entryName);
				strcpy(params[2], sortColumn);
				*/
				
				sem_wait(&semaphore);
				pthread_t tid = i;
				i++;
				sem_post(&semaphore);

				pthread_create(&tid, NULL, &goThroughCSV, (void*)csvtemp);
			}

			else if (okayDir(entryName)) {
				//increment thread counter
				//add thread id to array

				char readDir[99999];
				strcat(readDir, readDirName);
				strcat(readDir, "/");
				strcat(readDir, entryName);

				struct argstruct* dtemp = (struct argstruct*)malloc(sizeof(struct argstruct));
				
				strcpy(dtemp->arg1, readDir);
				strcpy(dtemp->arg2, sortColumn);
				//create thread, pass goThroughDir(readDir, sortColumn);
				/*char params[2][99999];
				strcpy(params[0], readDirName);
				strcpy(params[1], sortColumn);*/

				sem_wait(&semaphore);
				pthread_t tid = i;
				i++;
				sem_post(&semaphore);

				pthread_create(&tid, NULL, &goThroughDir, (void*)dtemp);
			} 
		}

		closedir(dir);
	}
}

int isCSV(char filename[]) {
	int l = strlen(filename);
	//printf("filename: %s", filename);

	if (l < 4) {
		return 0;
	}

	if (filename[l - 1] == 'v' && filename[l - 2] == 's' && filename[l - 3] == 'c') {
		return 1;
	}

	return 0;
}

int okayDir(char filename[]) {
	if (filename[0] == '.' && filename[1] == '.') {
		return 0;
	}

	if (filename[0] == '.') {
		return 0;
	}

	return 1;
}

void sort(char filename[], char sortColumn[], char entryName[]) {
	//printf("in sort\n");
	int foundColumn = 0;
	row *columnHeaders;
	columnHeaders = createRow();

	row *prevRow;
	row *currRow;
	prevRow = NULL;
	currRow = columnHeaders;
	
	int colCompare;
	colCompare = 0;
	int rowCount;
	rowCount = 0;

	FILE *fp;
	fp = fopen(filename, "r+");

	while (!feof(fp)) {
		char line[3000] = "";
		fgets(line, 3000, fp);

		char *string;
		char * delim = ",";
		string = strdupa(line);
		char * word = strsep(&string, delim);

		while (word != NULL) {

			if (!foundColumn && strcmp(word, sortColumn) == 0) {
				foundColumn = 1;
				colCompare = currRow->numCols;
			}

			while (word[0] && word[0] == '"' && word[strlen(word) - 1] && word[strlen(word) - 1] != '"') {

				char * newStr;
				newStr = malloc(strlen(word) + 1);
				newStr[0] = '\0';
				strcat(newStr, word);

				word = strsep(&string, delim);
				char * newStr2;
				newStr2 = malloc(strlen(word) + 1);
				newStr2[0] = '\0';
				strcat(newStr2, word);

				char * wholeWord;
				wholeWord = malloc(strlen(newStr) + strlen(newStr2) + 1);
				wholeWord[0] = '\0';
				strcat(wholeWord, newStr);
				strcat(wholeWord, newStr2);
				
				word =  malloc(strlen(wholeWord) + 1);
				word[0] = '\0';
				strcat(word, wholeWord);
			}

			trim(word);

			if (currRow->numCols == colCompare) {
				currRow->toCompare = malloc(strlen(word) + 1);
				strcpy(currRow->toCompare, word);
			}

			currRow->columns[currRow->numCols] = malloc(strlen(word) + 1);
			strcpy(currRow->columns[currRow->numCols], word);
			currRow->numCols = currRow->numCols + 1;

			word = strsep(&string, delim);
		}

		if (!foundColumn) {
			//printf("read: %s, write: %s, failed sort on: %s\n", filename, outputDirName, sortColumn);
			return;
		}

		if (prevRow != NULL && prevRow->numCols != currRow->numCols && currRow->numCols != 1) {
			//printf("failed getting data at: %s\n", filename);
			return;
		}

		currRow->numRow = rowCount;
		rowCount++;

		prevRow = currRow;
		currRow->next = createRow();
		currRow = currRow->next;
	}

	free(currRow);
	prevRow->next = NULL;

	fclose(fp);

	//create locks around this part
	sem_wait(&semaphore);
	globalListEnd->next = columnHeaders->next;
	globalListEnd = prevRow;
	sem_post(&semaphore);
}

row *createRow() {
	row *newRow;
	newRow = malloc(sizeof(row));
	newRow->numCols = 0;
	newRow->columns = malloc(sizeof(char*) * 999);
	newRow->toCompare = malloc(sizeof(char) + 1);
	newRow->numRow = 999999999;
	newRow->next = NULL;
	return newRow;
}

void trim(char * word) {
	int index;
	index = 0;
	while (word[index] == ' ' || word[index] == '\t' || word[index] == '\n' || word[index] == '"') {
		index++;
	}

	int newIndex;
	newIndex = 0;
	while (word[newIndex + index] != '\0') {
		word[newIndex] = word[newIndex + index];
		newIndex++;
	}
	word[newIndex] = '\0';

	newIndex = 0;
	index = -1;
	while (word[newIndex] != '\0') {
		if (word[newIndex] != ' ' && word[newIndex] != '\t' && word[newIndex] != '\n' && word[newIndex] != '"') {
			index = newIndex;
		}

		newIndex++;
	}

	word[index + 1] = '\0';
}
