#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <dirent.h>
#include <unistd.h>

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

void goThroughDir(char readDirName[], char outputDirName[], char sortColumn[]);
int isCSV(char filename[]);
int okayDir(char filename[]);
void sort(char filename[], char outputDirName[], char sortColumn[], char entryName[]);
void trim(char * word);
row *createRow();
row *mergesort(row * head);
row *merge(row * first, row * second);
