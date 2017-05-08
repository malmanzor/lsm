#include "config.h"
#include "fence.h"
#include <stdlib.h>
#include "value.h"
#include <stdio.h>

FencePointer * fnc_create(int maxSize){
    FencePointer * node = malloc(sizeof(FencePointer));
    node->keyCount = 0;
    node->keys = malloc(sizeof(int) * maxSize);
    return node;
}

void fnc_addNextMaximumForPage(FencePointer * node, int32_t maxKey){
    node->keys[node->keyCount++] = maxKey;
}

void fnc_setMinimum(FencePointer * node, int32_t minKey){
    node->minimum = minKey;
}

void fnc_setMaximum(FencePointer * node, int32_t maxKey){
    node->maximum = maxKey;
}

//Use binary search to find the page on disk where a key might be written to
static int fenceBinarySearch(int32_t * arr, int32_t key, int start, int end, int * address){
    if(start == end){
        if(arr[start] >= key){
            *address = start;
            return 1;
        }
        return 0;
    }

    if(end - start == 1){
        if(arr[start] >= key){
           *address = start;
            return 1;
        }else if(arr[end] >= key){
            *address = end;
            return 1;
        }
        return 0;
    }

    int pivot = (end + start) / 2;
    int32_t currentKey = arr[pivot];


    if(currentKey == key){
        *address = pivot;
        return 1;
    }else if(currentKey > key){
        end = pivot; //This could be the correct slot
    }else{
        start = pivot + 1;
    }
    return fenceBinarySearch(arr, key, start, end, address);

}


bool fnc_findAddress(FencePointer * node, int key, unsigned int * address){

    if(key < node->minimum &&
       key > node->maximum){
        return false;
    }

    int position;
    if(fenceBinarySearch(node->keys, key, 0, node->keyCount -1, &position)){
        *address = position * sizeof(LSMData) * PAGE_SIZE;
        return true;
    }

    return false;

}

void fnc_free(FencePointer * node){
    free(node->keys);
    free(node);
}



