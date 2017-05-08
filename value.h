#include <stdlib.h>
#include <stdint.h>
#pragma once

typedef int bool;
#define true 1
#define false 0

//Metadata about a key value pair
typedef enum {NORMAL_VALUE, DELETE, NOT_FOUND} ValueMetadata;

//Storage for a key value pair
typedef struct {
	int32_t key;
	int32_t value;
	ValueMetadata metadata;

} LSMData;

//Initialize LSM data for a put
void initHolder(LSMData * h, int32_t key, int32_t value);

//Initialize LSM data for a delete
void initDeleteHolder(LSMData * h, int32_t key);

