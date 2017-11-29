#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dirent.h>
#include <unistd.h>
#include <semaphore.h>
#include <pthread.h>

typedef struct row_ {
	char **columns;
	int numCols;
	char *toCompare;
	int numRow;
	struct row_ *next;
} row;

int countForks = 0;
int pids[99999];
int pi = 0;

int filesHave;
int filesNeed;
char file1[50] = "../MergeSort/sorterInput.csv";

void* goThroughDir(void * dirParams);
void* goThroughCSV(void * params);
int isCSV(char filename[]);
int okayDir(char filename[]);
void sort(char filename[], char sortColumn[], char entryName[]);
void trim(char * word);
row *createRow();
row *mergesort(row * head);
row *merge(row * first, row * second);

void addAllFiles(char* currDir);
void addFile(char* currDir, char* num);

row *globalListStart;
row *globalListEnd;

sem_t semaphore;
