#include <stdio.h>
#include "value.h"
#include "RangeResult.h"
#pragma once

/*
    Module with functionality for sorted arrays, either in memory or in a file on disk
*/

//Structure storing a sorted array
typedef struct {
	LSMData * arr;
	unsigned int maxValues;
	unsigned int currentValues;
	unsigned int deleteCount;
} SortedArray;

//Create a new array
SortedArray * sa_createSortedArray(int maxValues);

//Clear an array. After this function is run the array will be empty.
void sa_clear(SortedArray * arr);

//Free an array
void sa_free(SortedArray * arr);

//Add to an array, returns false if the array is full
bool sa_add(SortedArray * arr, int32_t key, int32_t value);

//Prints the summary for the array with a given label
void sa_printSummary(SortedArray * arr, const char * label);

//Searches for a key within the array. Returns NULL if
//a corresponding instance is not found
LSMData * sa_searchForValue(SortedArray * r, int32_t key);

//Search for a key in a sorted array file. Sets the value and metadata fields if the instances is found
//This function returns true if the instance is found, false otherwise
bool sa_searchForKeyInFile(FILE * f, int32_t key, int32_t * value, ValueMetadata * metadata);

//Print the summary for the data stored in the sorted array file.
void sa_printArrayFile(FILE * f, const char * label);

//Delete from a sorted array
bool sa_delete(SortedArray * arr, int32_t key);

//Scan a sorted array file for a range query
void sa_scanFileForRange(FILE * f, RangeResult* range, int32_t min, int32_t max);

//Split a full sorted array by adding data into a new array
void sa_splitArray(SortedArray * old, SortedArray * newArr);

