#pragma once
#include "config.h"
#include <stdint.h>
#include "value.h"

/*
    Fence pointer module used to make disk reads faster by mapping
    keys to addresses within a file. The keys should be added to the
    fence structure in the order they are written to disk. The structure
    uses the PAGE_SIZE to determine addresses on disk.
*/

//Fence pointer structure
typedef struct {
    int keyCount;
	int32_t * keys;
	int32_t minimum;
	int32_t maximum;
} FencePointer;

//Add the next key that is the maximum value for a page written to disk
void fnc_addNextMaximumForPage(FencePointer * node, int32_t maxKey);

//Set the minimum key for the file the fence pointer is indexing
void fnc_setMinimum(FencePointer * node, int32_t minKey);

//Set the maximum key for the file the fence pointer is indexing
void fnc_setMaximum(FencePointer * node, int32_t maxKey);

bool fnc_findAddress(FencePointer * node, int32_t key, unsigned int * address);

FencePointer * fnc_create(int maxSize);

void fnc_free(FencePointer * node);
