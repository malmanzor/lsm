#include "lsmlevel.h"
#include "config.h"
#include "atomics.h"
#include <stdlib.h>
#include <stdio.h>

LSMLevel * lvl_createLevel(unsigned int levelNumber, unsigned int ratio){
    LSMLevel * lvl = malloc(sizeof(LSMLevel));
    lvl->files = NULL;
    lvl->levelNumber = levelNumber;
    lvl->maxFiles = ratio;

    if(levelNumber == 1){
        lvl->maxFiles *= 2;
    }
    lvl->next = NULL;
    lvl->prev = NULL;
    lvl->mergeLatch = UNLOCKED;
    at_initAtomicCount(&(lvl->activeReadCount));
    return lvl;
}


ArrayOnDisk * lvl_createArrayOnDisk(unsigned long id, unsigned int maxEntries, float falsePositiveRate){
    ArrayOnDisk * newArr = malloc(sizeof(ArrayOnDisk));
    newArr->deleteCount = 0;
    newArr->id = id;
    newArr->size = 0;
    newArr->next = NULL;
    newArr->prev = NULL;
    newArr->filter = bf_create(maxEntries, falsePositiveRate);
    newArr->fence = fnc_create((maxEntries/PAGE_SIZE) + 1);
    return newArr;
}

ArrayOnDisk * lvl_getLastDiskArray(LSMLevel * level){
    ArrayOnDisk * arr = level->files;

    if(arr == NULL){
        return NULL;
    }

    while(arr->next != NULL){
        arr = arr->next;
    }
    return arr;
}

void lvl_clearLevel(LSMLevel * level){

    ArrayOnDisk * diskArr = level->files;
    while(diskArr != NULL){
        char filename[] = getFileName(level->levelNumber, diskArr->id);
        remove(filename);
        bf_free(diskArr->filter);
        fnc_free(diskArr->fence);

        ArrayOnDisk * rem = diskArr;
        diskArr = diskArr->next;
        free(rem);
    }
    level->files = NULL;

}

unsigned int lvl_getDiskArrayCount(LSMLevel * level){
    unsigned int count = 0;
    ArrayOnDisk * diskArr = level->files;
    while(diskArr != NULL){
        count++;
        diskArr = diskArr->next;
    }
    return count;
}

void lvl_addDiskArrayToLevel(LSMLevel * level, ArrayOnDisk * arr){
    if(level->files == NULL){
        level->files = arr;
    }else{
        ArrayOnDisk * existingArr = level->files;
        while(existingArr->next != NULL){
            existingArr = existingArr->next;
        }
        existingArr->next = arr;
        arr->prev = existingArr;
    }
}

