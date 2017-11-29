#include "sorter.h"
#include <sys/stat.h>
#include <sys/types.h>

int main(int argc, char** argv) {
	filesNeed = atoi(argv[1]);
	filesHave = 0;
	addAllFiles("tempFiles");
}

void addAllFiles(char* currDir) {
	if (filesHave >= filesNeed) {
		return;
	}

	int i;
	for (i = 0; i < 8; i++) {
		char filename[1000];
		strcpy(filename, currDir);
		strcat(filename, "/dir");

		char buffer[10];
		sprintf(buffer, "%d", i);

		strcat(filename, buffer);
		mkdir(filename, S_IRWXU);

		filesHave++;
		if (filesHave >= filesNeed) {
			return;
		}
	}

	addFile(currDir, "1");
	addFile(currDir, "2");

	filesHave += 2;
	if (filesHave >= filesNeed) {
		return;
	}

	char filename[1000];
	strcpy(filename, currDir);
	strcat(filename, "/dir0");
	
	addAllFiles(filename);
}

void addFile(char* currDir, char* num) {
	FILE *fp;
	fp = fopen(file1, "r");

	char filename[1000];
	strcpy(filename, currDir);
	strcat(filename, "/sortThis");
	strcat(filename, num);
	strcat(filename, ".csv");

	FILE *fp2;
	fp2 = fopen(filename, "w");

	while (!feof(fp)) {
		char line[3000] = "";
		fgets(line, 3000, fp);
		fputs(line, fp2);
	}

	fclose(fp);
	fclose(fp2);
}
