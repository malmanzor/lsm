#include <stdlib.h>
#include "bloom.h"
#include <stdio.h>
#include <stdint.h>
#include <math.h>
#include "value.h"

//bitarray based on http://www.mathcs.emory.edu/~cheung/Courses/255/Syllabus/1-C-intro/bit-array.html

#define setBitVector(arr,index) (arr[(index/32)] |= (1 << (index%32)) )

#define isBitSet(arr,index) (arr[(index/32)] & (1 << (index%32)) )

static unsigned int getFilterSizeInBits(int entries, float falsePositiveRate){
    return 2 * ceil(-(entries * log(falsePositiveRate))/pow(log(2),2));
}

static unsigned int getHashFunctionCount(float falsePositiveRate){
    return ceil(-(log(falsePositiveRate)/log(2)));
}

BloomFilter * bf_create(unsigned int entries, float falsePositiveRate){
	BloomFilter * filter = malloc(sizeof(BloomFilter));

	unsigned int arrSizeInBits = getFilterSizeInBits(entries, falsePositiveRate);
	unsigned int arrSizeInBytes = (arrSizeInBits/32) + 1;

	filter->bitArray = calloc(arrSizeInBytes,32);
	filter->arrSize = arrSizeInBits;
	filter->hashCount = getHashFunctionCount(falsePositiveRate);
	return filter;
}

void bf_free(BloomFilter * filter){
	free(filter->bitArray);
	free(filter);
}

//Implementation of the murmur hash 3 algorithm https://en.wikipedia.org/wiki/MurmurHash
unsigned int __attribute__((always_inline)) inline murmur3_32(int32_t key, unsigned int seed, unsigned int max) {
    const uint8_t * keyByte = (uint8_t *)&key;
    const uint32_t* key_x4 = (const uint32_t*) keyByte;
    uint32_t h = seed;

    uint32_t k = *key_x4++;
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    h ^= k;
    h = (h << 13) | (h >> 19);
    h += (h << 2) + 0xe6546b64;

    h ^= 4;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h % max;
}

void bf_add(BloomFilter * filter, int32_t key){
	int * bitArray = filter->bitArray;
	int max = filter->arrSize;
    int i;
    for(i = 0; i< filter->hashCount; i++){
        setBitVector(bitArray, murmur3_32(key, i, max));
    }
}

bool bf_contains(BloomFilter * filter, int32_t key){

	int * bitArray = filter->bitArray;
	int max = filter->arrSize;
	int i;
	for(i = 0; i < filter->hashCount; i++){
        if(!isBitSet(bitArray, murmur3_32(key, i, max))){
            return false;
        }
	}

	return true;
}
