#define _GNU_SOURCE

#include <stddef.h>
#include "mergesort.c"


int main(int argc, char** argv) {
	char readDirName[1000];
	char outputDirName[1000];
	char sortColumn[1000];
	
	//program called using -c flag only
	if (argc == 3) {
		if (strcmp(argv[1], "-c") != 0) {
			printf("Invalid arguements to sort\n");
			return 0;
		}

		strcpy(sortColumn, argv[2]);
		getcwd(readDirName, sizeof(readDirName));
		getcwd(outputDirName, sizeof(outputDirName));
		//printf("%s\n", readDirName);
	}

	//program called using -c and -d flags
	else if (argc == 5) {
		if (strcmp(argv[1], "-c") != 0 || strcmp(argv[3], "-d") != 0) {
			printf("Invalid arguements to sort\n");
			return 0;
		}

		strcpy(sortColumn, argv[2]);
		strcpy(readDirName, argv[4]);
		getcwd(outputDirName, sizeof(outputDirName));
		//printf("%s\n", readDirName);
	}

	//program called using -c, -d, and -o flags
	else if (argc == 7) {
		if (strcmp(argv[1], "-c") != 0 || strcmp(argv[3], "-d") != 0 || strcmp(argv[5], "-o") != 0) {
			printf("Invalid arguements to sort\n");
			return 0;
		}

		strcpy(sortColumn, argv[2]);
		strcpy(readDirName, argv[4]);
		strcpy(outputDirName, argv[6]);
	}

	//program called with unrecognized number of arguements
	else {
		printf("Invalid arguements to sort\n");
		return 0;
	}

	sem_init(&semaphore, 0, 1);

	/*
	if (getpid() == root) {
		printf("Initial PID: %d\n", root);
		printf("PIDS of all child processes: ");
	}
	*/

	globalListStart = createRow();
	globalListEnd = globalListStart;

	char parms[2][1000];
	strcpy(parms[0], readDirName);
	strcpy(parms[1], sortColumn);
	goThroughDir(parms);
	
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

/*
	if (getpid() == root) {
		printf("Initial PID: %d\n", root);
		
		printf("PIDS of all child processes: ");
		int i;
		for (i = 0; i < pi; i++) {
			printf("%d, ", pids[i]);
		}

		printf("\nTotal number of processes: %d\n", countForks + 1);
		//printf("pi: %d\n", pi);
	}
	*/
	
	return 0;
}


void* goThroughCSV(void * params) {


	//increment thread counter
	//add thread id to array

	char readName[99999];
	char **	temp = (char **) params;
	strcat(readName, temp[0]);
	strcat(readName, "/");
	strcat(readName, temp[1]);

	sort(readName, temp[2], temp[1]);
}

void* goThroughDir(void * dirParams) {

	char ** temp = (char **) dirParams;

	char readDirName[99999];
	strcpy(readDirName, temp[0]);

	char sortColumn[99999];
	strcpy(sortColumn, temp[1]);

	//printf("read: %s, write: %s, sort: %s\n", readDirName, outputDirName, sortColumn);
	DIR *dir;
	struct dirent *entry;

	int i = 0;
	if ((dir = opendir(readDirName)) != NULL) {
		//increment tid
		//add tid to array

		while ((entry = readdir(dir)) != NULL) {
			char entryName[256];
			strcpy(entryName, entry->d_name);

			//printf("entryName: %s, entry: %s", entryName, entry);
			if (isCSV(entryName)) {

				char params[3][99999];
				strcpy(params[0], readDirName);
				strcpy(params[1], entryName);
				strcpy(params[2], sortColumn);
				
				sem_wait(&semaphore);
				pthread_t tid = i;
				i++;
				sem_post(&semaphore);

				pthread_create(&tid, NULL, &goThroughCSV, params);
				//call go through CSV function

			}

			else if (okayDir(entryName)) {
				//increment thread counter
				//add thread id to array

				char readDir[99999];
				strcat(readDir, readDirName);
				strcat(readDir, "/");
				strcat(readDir, entryName);

				//create thread, pass goThroughDir(readDir, sortColumn);
				char params[2][99999];
				strcpy(params[0], readDirName);
				strcpy(params[1], sortColumn);

				sem_wait(&semaphore);
				pthread_t tid = i;
				i++;
				sem_post(&semaphore);

				pthread_create(&tid, NULL, &goThroughDir, params);
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
