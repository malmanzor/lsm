#pragma once

/*
    Configuration module containing defaults and functionality
    for reading user configuration

*/

//430
#define PAGE_SIZE 430

//#define TREE_RATIO 8

#define USE_FENCE 1

//#define LSM_DEBUG 1


#define EXPAND_DEBUG 1


//#define THREADED_MERGES 1

#define THREADED_READS 1

#define CONFIG_FILE_PATH "config.properties"

//Config keys that can be read from the config properties file
#define CONFIG_PROP_RATIO "lsm.ratio"
#define CONFIG_PROP_C0_SIZE "lsm.c0size"
#define CONFIG_PROP_THREAD_COUNT "lsm.threadCount"
#define CONFIG_PROP_BLOOM_FALSE_POSITIVE_RATE "lsm.bloomFPRate"

#define DEFAULT_RATIO 4
#define DEFAULT_C0_SIZE 2000000
#define DEFAULT_THREAD_COUNT 4
#define DEFAULT_FALSE_POSITIVE_RATE 0.01


//Configuration struct containing parameters for a LSM tree
typedef struct{
    unsigned int treeRatio;
    unsigned int c0size;
    unsigned int threadCount;
    float falsePositiveRate;
}LSMConfiguration;

//Determine configuration data using a configuration file and default
LSMConfiguration * cfg_readConfig();

//Free a configuration instance
void cfg_free(LSMConfiguration * config);
