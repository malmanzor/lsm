#pragma once
#include "value.h"

/*
    Module for storing and printing the results of a range query
*/

//A single piece of data that is part of a range query
typedef struct RangeResultInstanceNode{
    LSMData value;
    struct RangeResultInstanceNode * next;
    struct RangeResultInstanceNode * prev;
}RangeResultInstance;

//Structure to store the result of a range query
typedef struct{
    RangeResultInstance * head;
    RangeResultInstance * iter;
}RangeResult;

//Create a new RangeResult
RangeResult * rng_init();

//Add data to a range result. If the key is already within the range
//this data will be ignored
void rng_add(RangeResult * range, LSMData * value);

//Print the result of a range query
void rng_print(RangeResult * range);

//Free a RangeResult
void rng_free(RangeResult * range);

//Merge two RangeResults together. Any keys already in rangeToMergeInto will be ignored
void rng_merge(RangeResult * rangeToMergeInto, RangeResult * rangeToRead);
