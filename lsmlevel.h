#pragma once
#include "atomics.h"
#include "bloom.h"
#include "fence.h"

/*
    Module storing
*/

#define getFileName(l,c) {'d','a','t','a','/','c',(char)(((l/10)%10)+48),(char)(((l)%10)+48),'_',(char)((c/10000)%10+48),(char)((c/1000)%10+48),(char)((c/100)%10+48),(char)((c/10)%10+48),(char)((c%10)+48), '.','b','i','n', '\0'};

#define mergeLatchLevel(level) at_latchChar(&(level->mergeLatch))

#define mergeUnlatchLevel(level) at_unlatchChar(&(level->mergeLatch))

#define readLatchLevelOnceReadsAreComplete(level) at_waitForZeroAndLatch(&(level->activeReadCount))

#define addRead(level) at_addToCount(&(level->activeReadCount))

#define removeRead(level) at_decrementCount(&(level->activeReadCount))

#define readUnlatchLevel(level) at_unlatchChar(&(level->activeReadCount.latch))

//Struct that stores the logical representation of a file stored on disk
typedef struct ArrayOnDiskNode{
    unsigned int size;
    unsigned int deleteCount;
    BloomFilter * filter;
    unsigned long id;
    struct ArrayOnDiskNode * next;
    struct ArrayOnDiskNode * prev;

    #ifdef USE_FENCE

    FencePointer * fence;

    #endif // USE_FENCE

} ArrayOnDisk;

//Struct that represents a level of a LSM-Tree that stores its contents on disk
typedef struct LSMLevelNode{
    struct LSMLevelNode * next;
    struct LSMLevelNode * prev;
    AtomicCount activeReadCount;
    ArrayOnDisk * files;
    unsigned int levelNumber;
    int maxFiles;
    char mergeLatch;

} LSMLevel;

//Create a new level
LSMLevel * lvl_createLevel(unsigned int levelNumber, unsigned int ratio);

//Clear a level. This will delete files stored by the level.
void lvl_clearLevel(LSMLevel * level);

//Create a new ArrayOnDisk
ArrayOnDisk * lvl_createArrayOnDisk(unsigned long id, unsigned int maxEntries, float falsePositiveRate);

//Get the last ArrayOnDisk added to the level. This instance would be the most recently written.
ArrayOnDisk * lvl_getLastDiskArray(LSMLevel * level);

//Get the count of files stored in the level
unsigned int lvl_getDiskArrayCount(LSMLevel * level);

//Add a new file to the level
void lvl_addDiskArrayToLevel(LSMLevel * level, ArrayOnDisk * arr);
