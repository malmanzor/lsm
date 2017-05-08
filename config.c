#include <stdio.h>
#include <stdlib.h>
#include "config.h"
#include <string.h>

#define MAX_READ_SIZE 500

LSMConfiguration * cfg_readConfig(){
    FILE * configFile = fopen(CONFIG_FILE_PATH, "r");

    if(configFile == NULL){
        printf("could not find %s\n", CONFIG_FILE_PATH);
        return NULL;
    }


    unsigned int ratio = DEFAULT_RATIO;
    unsigned int c0size = DEFAULT_C0_SIZE;
    unsigned int threadCount = DEFAULT_THREAD_COUNT;
    float falsePositiveRate = DEFAULT_FALSE_POSITIVE_RATE;

    char line[MAX_READ_SIZE];
    char * key = NULL;//malloc((MAX_READ_SIZE/2) * sizeof(char));
    char * value = NULL;//malloc((MAX_READ_SIZE/2) * sizeof(char));

    while( fgets(line, MAX_READ_SIZE, configFile ) != NULL){
        if('#' == line[0]){
            continue;
        }

        key = strtok (line,"=");
        if(key != NULL){
            value = strtok(NULL,"=");
            if(strcmp(key,CONFIG_PROP_RATIO) == 0){
                ratio = atoi(value);
            }else if(strcmp(key,CONFIG_PROP_C0_SIZE) == 0){
                c0size = atoi(value);
            }else if(strcmp(key,CONFIG_PROP_THREAD_COUNT) == 0){
                threadCount = atoi(value);
            }else if(strcmp(key,CONFIG_PROP_BLOOM_FALSE_POSITIVE_RATE) == 0){
                falsePositiveRate = atof(value);
            }
        }


    }

    LSMConfiguration * config = malloc(sizeof(LSMConfiguration));
    config->treeRatio = ratio;
    config->c0size = c0size;
    config->threadCount = threadCount;
    config->falsePositiveRate = falsePositiveRate;
    fclose(configFile);
    return config;
}

void cfg_free(LSMConfiguration * config){
    free(config);
}
