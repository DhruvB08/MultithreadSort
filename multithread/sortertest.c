#define _GNU_SOURCE

#include <semaphore.h>
#include <pthread.h>
#include <stddef.h>
#include "mergesort.c"

int pc = 0; 
pid_t p;

// allocates memory for subpaths and also appends /
char* pathcat(const char* str1,const char* str2){ 
    char* subpath;  
    subpath=(char*)malloc(strlen(str1)+strlen(str2)+ 3);

    if(subpath == NULL){
        printf("failed to allocate memory\n");  
        exit(1);  
    }  
	strcpy(subpath,str1);
	strcat(subpath,"/");   
	strcat(subpath,str2);  
	return subpath;  
} 

void pcounter(char* path){ // , char* colsort
    DIR *dir;
    dir = opendir(path);
    struct dirent *sd;
    
    if(dir == NULL) 
    {
        printf("Error: Directory N/A");
        exit(1);
    }
    
    while ((sd = readdir(dir)) != NULL){
	char* subpath;
	int length = strlen(sd->d_name); 
	subpath = pathcat(path, sd->d_name);
	//struct stat s;
	//stat(subpath, &s);
       
        if(((sd->d_type) == DT_DIR) && (strcmp(sd->d_name, ".") !=0) && (strcmp(sd->d_name, "..") !=0)){
            pc++;
            pcounter(subpath); // , colsort
        }else if(((sd->d_type) == DT_REG) && (strncmp(sd->d_name+length-4, ".csv", 4) == 0)){
            pc++;
	}
    }
}

void goThroughDir(char readDirName[], char outputDirName[], char sortColumn[]) {
	//printf("read: %s, write: %s, sort: %s\n", readDirName, outputDirName, sortColumn);
	DIR *dir;
	struct dirent *entry;

	if ((dir = opendir(readDirName)) != NULL) {
		pid_t pid = 1;

		while ((entry = readdir(dir)) != NULL) {
			char entryName[256];
			strcpy(entryName, entry->d_name);

			//printf("entryName: %s, entry: %s", entryName, entry);
			if (isCSV(entryName)) {
				//printf("sorting: %s\n", entryName);
				countForks += 1;
				pid = fork();
				//printf("sorting, forked, pid: %d\n", pid);
				if (pid == 0) {
					//printf("%d, ", getpid());
					pids[pi] = getpid();
					pi++;

					char outputDirName1[99999];
					if (outputDirName[0] != '/') {
						//strcat(outputDirName1, "../");
					}
					strcat(outputDirName1, outputDirName);

					char readName[99999];
					strcat(readName, readDirName);
					strcat(readName, "/");
					strcat(readName, entryName);
					
					//printf("sorting: %s, write: %s, sort: %s\n", entryName, outputDirName1, sortColumn);
					sort(readName, outputDirName1, sortColumn, entryName);
					return;
				}
			}
			else if (okayDir(entryName)) {
				countForks += 1;
				pid = fork();
				//printf("going to subdir, pid: %d\n", pid);
				if (pid == 0) {
					//printf("%d, ", getpid());
					pids[pi] = getpid();
					pi++;

					char outputDirName2[99999];
					if (outputDirName[0] != '/') {
						//strcat(outputDirName2, "../");
					}
					strcat(outputDirName2, outputDirName);
					
					char readDir[99999];
					strcat(readDir, readDirName);
					strcat(readDir, "/");
					strcat(readDir, entryName);
					
					goThroughDir(readDir, outputDirName2, sortColumn);
					return;
				} 
			}
		}

		if (pid == 0) {
			//wait();
		}

		closedir(dir);
	}
	else {
		//printf("Couldn't open dir: %s\n", readDirName);
		//countForks--;
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

void sort(char filename[], char outputDirName[], char sortColumn[], char entryName[]) {
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

	fclose(fp);

	char writeFile[99999];
	strcpy(writeFile, outputDirName);
	strcat(writeFile, "/");
	strncat(writeFile, entryName, strlen(entryName) - 4);
	strcat(writeFile, "-sorted-");
	strcat(writeFile, sortColumn);
	strcat(writeFile, ".csv");

	//printf("read: %s, write: %s\n", filename, writeFile);
	fp = fopen(writeFile, "w+");

	free(currRow);
	prevRow->next = NULL;

	currRow = mergesort(columnHeaders->next);
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

	free(columnHeaders);
	fclose(fp);
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

	int root = getpid();
	int troot = pthread_self();
	printf("Initial TID: %d\n", troot); // just to test out initial thread

	/*
	if (getpid() == root) {
		printf("Initial PID: %d\n", root);
		printf("PIDS of all child processes: ");
	}
	*/

	goThroughDir(readDirName, outputDirName, sortColumn);
	
	if (getpid() == root) {
		printf("Initial PID: %d\n", root);
		
		printf("PIDS of all child processes: ");
		int i;
		for (i = 0; i < pi; i++) {
			printf("%d, ", pids[i]);
		}
		pcounter(".");
		printf("\nTotal # processes: %d\n", pc+1);
	}
	
	return 0;
}
