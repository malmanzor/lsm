#include "LSM.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdint.h>

#define PARSE_ERROR 1
#define MAX_READ_SIZE 500

//Load puts from a data file
void loadFromFile(LSMTree * tree, FILE * loadFile){
    int32_t key;
    int32_t value;
    while(fread(&key,4,1,loadFile) == 1 && fread(&value,4,1,loadFile) == 1){
        lsm_put(tree, key, value);
    }
}

int prs_parseLSMCommands(LSMTree * tree, FILE * input){

	char line[MAX_READ_SIZE];

	while( fgets(line, MAX_READ_SIZE, input ) != NULL){
		const char * command = &line[0];
		//check for content
		if('p' == line[0]){
			int32_t key;
			int32_t value;

			if(2 != sscanf(line, "p %i %i", &key, &value)){

				printf("Invalid insert command, two arguments are required");
				return PARSE_ERROR;
			}

			lsm_put(tree, key, value);
		}else if('s' == line[0]){
			lsm_printSummary(tree);
		}else if('g' == line[0]){
			int32_t key;
			if(1 != sscanf(line, "g %i", &key)){
				printf("Invalid get command, one integer argument is required");
				return PARSE_ERROR;
			}
			lsm_getAndPrint(tree, key);
		}else if('d' == line[0]){
		    int32_t key;
			if(1 != sscanf(line, "d %i", &key)){
				printf("Invalid delete command, one integer argument is required");
				return PARSE_ERROR;
			}
			lsm_delete(tree, key);
		} else if('r' == line[0]){
            int32_t min;
			int32_t max;

			if(2 != sscanf(line, "r %i %i", &min, &max)){

				printf("Invalid range command, two arguments are required");
				return PARSE_ERROR;

			}

			lsm_printRange(tree, min, max);
		} else if('l' == line[0]){
            char * fileName = malloc(256 * sizeof(char));
            if(1 != sscanf(line, "l \"%s", fileName)){
				printf("Invalid load command, one filename argument in double quotes is required");
				return PARSE_ERROR;
            }

            //End the string when we find the final double quote
            int i;
            for(i = 0; i<strlen(fileName); i++){
                if(fileName[i] == '"'){
                    fileName[i] = '\0';
                }
            }

            FILE * loadFile = fopen(fileName, "r");

            if(loadFile == NULL){
                printf("Could not read from file %s\n", fileName);
                free(fileName);
                return PARSE_ERROR;
            }
            free(fileName);
            loadFromFile(tree, loadFile);
            fclose(loadFile);
		}else if(isspace(line[0])){
            continue;
		}else{
			printf("unknown command: %s", command);
			return PARSE_ERROR;
		}
	}
	return 0;
}



