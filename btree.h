#pragma once
#include "value.h"
#include "SortedArray.h"
#include "RangeResult.h"

/*
    Single level B-Tree that is used to store instances of LSMData

*/

//The main node of the B-Tree
typedef struct{
    //Current number of leaves
    int leafCount;

    //Maximum number of leaves
    int maxLeaves;

    //Maximum key for each page
    int32_t * keys;

    //Mapping of the index of a key in the keys array to the index
    //Of the page in the leaves array
    int * indexesOfArrays;

    //Leaf nodes
    SortedArray * leaves;

    //Array of data that is accessed through the SortedArrays in the leaves
    //array
    LSMData * data;
}BTree;

//An iterator for traversing the tree
typedef struct{
    BTree * tree;
    SortedArray * currentArray;
    int arrayIndex;
    int index;

}BTreeIterator;

//Create a B-Tree
BTree * bt_create(int maximum);

//Add to a B-Tree
bool bt_insert(BTree * tree, int32_t key, int32_t value);

//Delete from a B-Tree
bool bt_delete(BTree * tree, int32_t key);

//Free a B-Tree
void bt_freeTree(BTree * tree);

//Clear a B-Tree. This removes the contents but does not deallocate memory.
//Once a tree has been cleared it can be treated as a newly initialized tree.
void bt_clearTree(BTree * tree);

//Create an iterator to traverse the tree
BTreeIterator * bt_createIterator(BTree * tree);

//Get the next value from the iterator or NULL if the iterator is finished
LSMData * bt_iterNext(BTreeIterator * iter);

//Free the iterator
void bt_freeIterator(BTreeIterator * iter);

//Get the count of non deleted instances in the B-Tree.
unsigned int bt_getCount(BTree * tree);

//Search for a key in the tree. This function will return the found instance or
//NULL
LSMData * bt_search(BTree * tree, int32_t key);

//Add the contents of the B-Tree to a RangeResult that fall within the min and max
void bt_scanForRange(BTree * tree, RangeResult * range, int32_t min, int32_t max);

//Get the maximum number of entries the B-Tree can hold
unsigned int bt_maxEntries(BTree * tree);

