#include "btree.h"
#include "SortedArray.h"
#include "config.h"
#include "RangeResult.h"
#include <string.h>

#define LEAF_SIZE 400

#define getArray(tree,i) &(tree->leaves[tree->indexesOfArrays[i]])

BTree * bt_create(int maximum){
    BTree * bt = malloc(sizeof(BTree));
    bt->leafCount = 0;
    bt->maxLeaves = (maximum / LEAF_SIZE ) + 1;
    bt->keys = malloc(sizeof(int) * bt->maxLeaves);
    bt->indexesOfArrays = malloc(sizeof(int) * bt->maxLeaves);
    bt->leaves = malloc(sizeof(SortedArray) * bt->maxLeaves);
    bt->data = calloc(LEAF_SIZE * bt->maxLeaves, sizeof(LSMData));

    int i;
    for(i = 0; i < bt->maxLeaves; i++){
        SortedArray * arr = &(bt->leaves[i]);
        arr->arr = &(bt->data[LEAF_SIZE * i]);
        //arr->arr = calloc(LEAF_SIZE, sizeof(LSMData));
        arr->maxValues = LEAF_SIZE;
        arr->currentValues = 0;
        arr->deleteCount = 0;
    }

    return bt;
}

//Use binary search to find where a key should be stored
//The return value can be used as an index into indexesOfArrays
//to find the correct SortedArray in the leaves array.
static int findInKeyArray(BTree * tree, int32_t key){
    int start = 0;
    int end = tree->leafCount -1;
    int32_t * keySet = tree->keys;
    while(1){

        if(start == end){
            return start;
        }
        if(end - start == 1){
            if(keySet[start] < key){
                return end;
            }else{
                return start;
            }

        }

        int pivot = (end+start)/2;
        int pivotValue = keySet[pivot];

        if(pivotValue == key){
            return pivot;
        }else if (pivotValue > key){
            end = pivot;
        }else{
            start = pivot + 1;
        }

    }

}

//Expand a B-Tree node that is full
static SortedArray * expandNode(BTree * tree, SortedArray * target, int targetIndex){

    //Create the new node and populate it
    SortedArray * arrayToCopyInto = &(tree->leaves[tree->leafCount]);
    sa_splitArray(target, arrayToCopyInto);

    //Make space for the new key and index
    memmove(&(tree->keys[targetIndex+2]), &(tree->keys[targetIndex+1]),
			sizeof(int32_t)
			* (tree->leafCount -1  - targetIndex));

    memmove(&(tree->indexesOfArrays[targetIndex+2]), &(tree->indexesOfArrays[targetIndex+1]),
			sizeof(int)
			* (tree->leafCount -1  - targetIndex));

    //Fix the keys and indexes arrays
    tree->indexesOfArrays[targetIndex+1] = tree->leafCount++;
    tree->keys[targetIndex + 1] = arrayToCopyInto->arr[arrayToCopyInto->currentValues-1].key;
    tree->keys[targetIndex] = target->arr[target->currentValues-1].key;

    return arrayToCopyInto;
}

bool bt_insert(BTree * tree, int32_t key, int32_t value){
    if(tree->leafCount == 0){
        tree->leafCount = 1;
        tree->keys[0] = key;
        tree->indexesOfArrays[0] = 0;
        SortedArray * arr = &(tree->leaves[0]);
        arr->currentValues = 0;
        arr->deleteCount = 0;
        sa_add(arr, key, value);
        return true;
    }

    int targetIndex = findInKeyArray(tree, key);

    SortedArray * target = getArray(tree, targetIndex);
    if(!sa_add(target, key, value)){
        if(tree->leafCount == tree->maxLeaves){
                return false;
        }

        SortedArray * newArr = expandNode(tree, target, targetIndex);

        if(target->arr[target->currentValues-1].key > key){
            sa_add(target, key, value);
        }else{
            sa_add(newArr, key, value);
        }
        return true;

    }

    tree->keys[targetIndex] = target->arr[target->currentValues-1].key;
    return true;
}

bool bt_delete(BTree * tree, int32_t key){
    if(tree->leafCount == 0){
        tree->leafCount = 1;
        tree->keys[0] = key;
        tree->indexesOfArrays[0] = 0;
        SortedArray * arr = &(tree->leaves[0]);
        arr->currentValues = 0;
        arr->deleteCount = 0;
        sa_delete(arr, key);
        return true;
    }

    int targetIndex = findInKeyArray(tree, key);

    SortedArray * target = getArray(tree, targetIndex);
    if(!sa_delete(target, key)){
        if(tree->leafCount == tree->maxLeaves){
                return false;
        }

        SortedArray * newArr = expandNode(tree, target, targetIndex);

        if(target->arr[target->currentValues-1].key > key){
            sa_delete(target, key);
        }else{
            sa_delete(newArr, key);
        }
        return true;
    }

    tree->keys[targetIndex] = target->arr[target->currentValues-1].key;
    return true;
}

void bt_freeTree(BTree * tree){
    free(tree->indexesOfArrays);
    free(tree->keys);
    free(tree->leaves);
    free(tree->data);
    free(tree);
}

void bt_clearTree(BTree * tree){
    tree->leafCount = 0;
}

BTreeIterator * bt_createIterator(BTree * tree){
    BTreeIterator * iter = malloc(sizeof(BTreeIterator));
    iter->tree = tree;
    iter->currentArray = NULL;
    iter->arrayIndex = 0;
    iter->index = 0;
    return iter;
}



LSMData * bt_iterNext(BTreeIterator * iter){

    //Initialize the iterator if needed
    if(iter->currentArray == NULL){
        if(iter->tree->leafCount == 0){
            return NULL;
        }else{
            iter->currentArray = getArray(iter->tree, 0);
        }
    }

    //Are we at the end of this array?
    if(iter->index >= iter->currentArray->currentValues){
            iter->index = 0;
        //Are there no more leaves?
        if(++iter->arrayIndex >= iter->tree->leafCount){
            return NULL;
        }else{
            iter->currentArray = getArray(iter->tree, iter->arrayIndex);
        }
    }

    //Go to the next instance within an array
    return &(iter->currentArray->arr[iter->index++]);
}

void bt_freeIterator(BTreeIterator * iter){
    free(iter);
}

unsigned int bt_getCount(BTree * tree){
    int i;
    unsigned int total = 0;
    for(i = 0; i < tree->leafCount; i++){
        SortedArray * current = getArray(tree, i);
        total += current->currentValues - current->deleteCount;
    }
    return total;
}

LSMData * bt_search(BTree * tree, int32_t key){
    if(tree->leafCount == 0){
        return NULL;
    }
    int targetIndex = findInKeyArray(tree, key);
    SortedArray * target = getArray(tree, targetIndex);
    return sa_searchForValue(target, key);
}

void bt_scanForRange(BTree * tree, RangeResult * range, int32_t min, int32_t max){

    BTreeIterator * iter = bt_createIterator(tree);

    LSMData * data = bt_iterNext(iter);
    while(data != NULL){
        if(data->key >= max){
            break;
        }
        if(data->key >= min){
            rng_add(range, data);
        }
        data = bt_iterNext(iter);
    }
    bt_freeIterator(iter);
}

unsigned int bt_maxEntries(BTree * tree){
    return tree->maxLeaves * LEAF_SIZE;
}
