#pragma once
#include <stdio.h>
#include "btree.h"
#include "atomics.h"
#include "lsmlevel.h"
#include "threadpool.h"
#include "output.h"
#include <stdint.h>

/*
    Module defining a LSM-Tree
*/

//LSM struct
typedef struct {

    BTree * activeC0;
    BTree * inactiveC0;

    volatile char c0mergeLatch;
    AtomicCount c0reads;
    LSMLevel * levelsOnDisk;
    ThreadPool * threadPool;
    OutputBuffer * buffer;

    //Ratio between levels
    unsigned int treeRatio;

    unsigned int c0size;

    //False positive rate for bloom filters
    float falsePositiveRate;
    unsigned long fileId;
}LSMTree;

//Create a LSM-Tree
LSMTree * lsm_create(LSMConfiguration * config);

//Add a key value pair to a LSM-Tree
void lsm_put(LSMTree * tree, int32_t key, int32_t value);

//Print the value associated with the key
void lsm_getAndPrint(LSMTree * tree, int32_t key);

//Print the summary for the tree
void lsm_printSummary(LSMTree * tree);

//Free the LSM-Tree
void lsm_free(LSMTree * tree);

//Delete a key from the LSM-Tree
void lsm_delete(LSMTree * tree, int32_t key);

//Print a range of keys stored in a LSM-Tree
void lsm_printRange(LSMTree * tree, int32_t min, int32_t max);

