#pragma once
#include <stdint.h>
#include "value.h"

/*
    A Bloom Filter used to determine if a collection definitely does not contain a key.
    It is a probabilistic data structure that can return false positives.
*/

//Structure containing the fields required by a Bloom Filter
typedef struct {
	int * bitArray;
	unsigned int arrSize;
	unsigned int hashCount;
	float falsePositiveRate;
} BloomFilter;

//Create a new Bloom Filter
BloomFilter * bf_create(unsigned int entries, float falsePositiveRate);

//Free a Bloom Filter
void bf_free(BloomFilter * filter);

//Add a key to a Bloom Filter
void bf_add(BloomFilter * filter, int32_t key);

//Check to see if the Bloom Filter contains a key
bool bf_contains(BloomFilter * filter, int32_t key);
