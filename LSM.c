#include <stdlib.h>
#include <stdio.h>
#include "SortedArray.h"
#include "config.h"
#include <limits.h>
#include "bloom.h"
#include "btree.h"
#include "tree.h"
#include "lsmlevel.h"
#include "atomics.h"
#include "fence.h"
#include "LSM.h"
#include "lsmlevel.h"
#include <math.h>

#ifdef EXPAND_DEBUG

#include <sys/time.h>

#endif // EXPAND_DEBUG

#define getLevelName(l) {'L',(char)((l%100)+48), (char)((l%10)+48),'\0' }

#define mergeLatchC0(tree) at_latchChar(&(tree->c0mergeLatch))

#define mergeUnlatchC0(tree) at_unlatchChar(&(tree->c0mergeLatch))

#define readLatchC0onceReadsAreComplete(tree) at_waitForZeroAndLatch(&(tree->c0reads))

#define readUnlatchC0(tree) at_unlatchChar(&(tree->c0reads.latch))

#define addC0Read(tree) at_addToCount(&(tree->c0reads))

#define removeC0Read(tree) at_decrementCount(&(tree->c0reads))


LSMTree * lsm_create(LSMConfiguration * config){
	LSMTree * tree = malloc(sizeof(LSMTree));
    tree->c0size = config->c0size;
    tree->treeRatio = config->treeRatio;

    //Get the size for the c0trees
    int treeSizes = (tree->c0size / 2) + 1;
    tree->activeC0 = bt_create(treeSizes);
    tree->inactiveC0 = bt_create(treeSizes);

    tree->levelsOnDisk = NULL;
    tree->c0mergeLatch = UNLOCKED;
    at_initAtomicCount(&(tree->c0reads));
    tree->threadPool = tp_create(config->threadCount - 1);
    tree->buffer = out_initBuffer();
    tree->falsePositiveRate = config->falsePositiveRate;
    tree->fileId = 1;
	return tree;
}

//Disk readers are helpers for reading data from a file during a merge
typedef struct DiskReaderNode{
    FILE * f;
    bool open;
    LSMData current;
    struct DiskReaderNode * next;
} DiskReader;

//Advance the disk reader. Returns false if the file is complete
static bool diskReaderRead(DiskReader * reader){

    if(!reader->open){
        return false;
    }

    if(fread(&(reader->current), sizeof(LSMData), 1, reader->f) != 1){
        reader->open = false;
        fclose(reader->f);
        return false;
    }
    return true;
}

//Get a level. LSMLevels start at level number 1. If the level does not exist in the tree
//a new LSMLevel instance will be created
static LSMLevel * getLevelAndCreateIfRequired(LSMTree * tree, int levelToGet){
    LSMLevel * lvl = tree->levelsOnDisk;
    int i;
    for(i = 1; i< levelToGet; i++){
        lvl = lvl->next;
    }

    if(lvl == NULL){
        lvl = lvl_createLevel(levelToGet, tree->treeRatio);
    }

    return lvl;
}

//A helper for writing to a file during a merge
//The Write handler manages the file being written to
//as well as adding to bloom filters and fence pointers
typedef struct{
    FILE * fileToWriteTo;
    int32_t lastKey;
    int countWritten;
    int deleteCount;
    ArrayOnDisk * diskArray;
}WriteHandler;

//Create a new WriteHandler
static void writeHandlerInit(WriteHandler * writer, FILE * target, ArrayOnDisk * diskArray){
    writer->deleteCount = 0;
    writer->lastKey = 0;
    writer->countWritten = 0;
    writer->fileToWriteTo = target;
    writer->diskArray = diskArray;
}

//Finish a write handler. This should be called after all writes to the file are complete
static void finishWriteHandler(WriteHandler * writer){

    #ifdef USE_FENCE

    if(writer->countWritten < PAGE_SIZE){
        fnc_addNextMaximumForPage(writer->diskArray->fence,writer->lastKey);
    }

    fnc_setMaximum(writer->diskArray->fence, writer->lastKey);

    #endif // USE_FENCE

    writer->diskArray->size = writer->countWritten;
    writer->diskArray->deleteCount = writer->deleteCount;

    fclose(writer->fileToWriteTo);
}

//Write data to the file
static void write(WriteHandler * writer, LSMData * data){

        fwrite(data, sizeof(LSMData), 1, writer->fileToWriteTo);
        bf_add(writer->diskArray->filter, data->key);
        writer->lastKey = data->key;
        writer->countWritten++;

        if(data->metadata == DELETE){
            writer->deleteCount++;
        }

        #ifdef USE_FENCE
        if(writer->countWritten == 1){
            fnc_setMinimum(writer->diskArray->fence, writer->lastKey);
        }

        if(writer->countWritten % PAGE_SIZE == 0){
            fnc_addNextMaximumForPage(writer->diskArray->fence,writer->lastKey);
        }

        #endif // USE_FENCE


}

//Input to a merge function
typedef struct{
    LSMTree * tree;
    int levelToReadFrom;
    unsigned long fileId;
} MergeInput;

/*
    Write from the inactive c0 tree into a new file in c1

    Precondition: The merge latch for c0 is held
*/
static void * writeToC1(void * arg){
    MergeInput * input = (MergeInput *) arg;
    LSMTree * tree = input->tree;
    unsigned long fileid = input->fileId;
    BTreeIterator * iter = bt_createIterator(tree->inactiveC0);
    LSMLevel * levelToWriteTo = getLevelAndCreateIfRequired(tree, 1);

    ArrayOnDisk * diskArray = lvl_createArrayOnDisk(fileid, bt_maxEntries(tree->inactiveC0) + 1, tree->falsePositiveRate);
    char filename[] = getFileName(1, fileid);

    WriteHandler writer;
    writeHandlerInit(&writer, fopen(filename, "w"), diskArray);

    LSMData * data = bt_iterNext(iter);

    while(data != NULL){

        write(&writer, data);

        data = bt_iterNext(iter);
    }

    finishWriteHandler(&writer);

    bt_freeIterator(iter);

    mergeLatchLevel(levelToWriteTo);
    readLatchLevelOnceReadsAreComplete(levelToWriteTo);

    lvl_addDiskArrayToLevel(levelToWriteTo, diskArray);

    //If this is a new level, add it to the tree
    if(tree->levelsOnDisk == NULL){
        tree->levelsOnDisk = levelToWriteTo;
    }

    readUnlatchLevel(levelToWriteTo);
    mergeUnlatchLevel(levelToWriteTo);

    readLatchC0onceReadsAreComplete(tree);

    //All data from the inactive c0 has been written to disk
    //so the tree can be cleared
    bt_clearTree(tree->inactiveC0);

    readUnlatchC0(tree);
    mergeUnlatchC0(tree);

    free(arg);
    return NULL;

}

/*
    Write to a disk level besides c1. This will involve reading data written to disk in the previous level
    and merging it into a new run

    Precondition: The merge latch for the level to read from is held
*/
static void * writeToLevelFromDisk(void * arg){
    MergeInput * in = (MergeInput *) arg;
    LSMTree * tree = in->tree;
    int levelToWriteFrom = in->levelToReadFrom;
    unsigned long fileid = in->fileId;
    LSMLevel * levelToRead = getLevelAndCreateIfRequired(tree, levelToWriteFrom);
    LSMLevel * levelToWriteTo = getLevelAndCreateIfRequired(tree, levelToWriteFrom + 1);
    ArrayOnDisk * diskArray = lvl_createArrayOnDisk(fileid, ceil(pow(tree->treeRatio, levelToWriteFrom)) * ((bt_maxEntries(tree->inactiveC0) * 2) + 1), tree->falsePositiveRate);
    char filename[] = getFileName((levelToWriteFrom + 1), fileid);

    WriteHandler writer;
    writeHandlerInit(&writer,fopen(filename, "w"), diskArray);

    ArrayOnDisk * diskArrayToRead = lvl_getLastDiskArray(levelToRead);

    DiskReader * firstReader = NULL;
    DiskReader * reader = NULL;

    //Initialize a list of readers, one for each file in the previous level
    while(diskArrayToRead != NULL){
        DiskReader * newReader = malloc(sizeof(DiskReader));
        initHolder(&(newReader->current),0,0);
        char filenameForRead[] = getFileName(levelToRead->levelNumber, diskArrayToRead->id);
        newReader->f = fopen(filenameForRead, "r");
        newReader->open = 1;
        diskReaderRead(newReader);
        newReader->next = NULL;

        if(firstReader == NULL){
            firstReader = newReader;
        }

        if(reader != NULL){
            reader->next = newReader;
        }
        reader = newReader;
        diskArrayToRead = diskArrayToRead->prev;
    }

    int hasMore = 1;

    LSMData * currentValue;
    LSMData * minimum;
    DiskReader * readerWithMinKey;

    //Read through the files adding the unique keys from smallest to largest
    //Check the newer files first so that the freshest data is written to disk
    while(hasMore){
        hasMore = 0;

        reader = firstReader;
        minimum = NULL;
        readerWithMinKey = NULL;
        long minKey = LONG_MAX;
        while(reader != NULL){
            if(reader->open){
                currentValue = &(reader->current);
                int currentKey = currentValue->key;
                if(currentKey < minKey){
                    minKey = currentKey;
                    minimum = currentValue;

                    if(readerWithMinKey != NULL){
                        //Another file has a key with a higher value that needs to be written
                        hasMore++;
                    }
                    readerWithMinKey = reader;
                }else if(currentKey == minKey){
                    //This is an overridden value so we can ignore it
                    hasMore += diskReaderRead(reader);
                }else{
                    hasMore += 1;
                }
            }
            reader = reader->next;
        }
        if(minimum != NULL){
            write(&writer, minimum);
            hasMore += diskReaderRead(readerWithMinKey);
        }
    }

    finishWriteHandler(&writer);

    reader = firstReader;
    while(reader != NULL){
        if(reader->open){
            fclose(reader->f);
        }

        DiskReader * rem = reader;
        reader = reader->next;
        free(rem);
    }

    mergeLatchLevel(levelToWriteTo);
    readLatchLevelOnceReadsAreComplete(levelToWriteTo);

    lvl_addDiskArrayToLevel(levelToWriteTo, diskArray);

    readUnlatchLevel(levelToWriteTo);
    mergeUnlatchLevel(levelToWriteTo);

    readLatchLevelOnceReadsAreComplete(levelToRead);

    //If this is a new level add it to the tree
    if(levelToRead->next == NULL){
        levelToRead->next = levelToWriteTo;
    }

    //All data from the previous level has been written. The data can be deleted
    lvl_clearLevel(levelToRead);

    readUnlatchLevel(levelToRead);
    mergeUnlatchLevel(levelToRead);

    free(arg);
    return NULL;
}

MergeInput * createMergeInput(LSMTree * tree, int levelToReadFrom){
     MergeInput * input = malloc(sizeof(MergeInput));
    input->tree = tree;
    input->levelToReadFrom = levelToReadFrom;
    input->fileId = tree->fileId++;
    return input;
}

//This function merges levels of the tree so that new data can be written.
static void expandTree(LSMTree * tree){

    #ifdef EXPAND_DEBUG

    struct timeval tv;

    gettimeofday(&tv, NULL);

    unsigned long long millisecondsSinceEpochPre =
    (unsigned long long)(tv.tv_sec) * 1000 +
    (unsigned long long)(tv.tv_usec) / 1000;

    #endif // EXPAND_DEBUG

    //We must wait for previous merges to be complete first
    mergeLatchC0(tree);

    readLatchC0onceReadsAreComplete(tree);

    //Switch active and inactive c0 trees so that writes can continue
    //while the merge is in progress
    BTree * temp = tree->activeC0;
    tree->activeC0 = tree->inactiveC0;
    tree->inactiveC0 = temp;

    readUnlatchC0(tree);

    int fullLevelToMerge = 0;
    LSMLevel * level = tree->levelsOnDisk;

    //Determine how many full levels there are
    while(level != NULL){
        int diskArrCount = lvl_getDiskArrayCount(level);
        if(diskArrCount < level->maxFiles){
            break;
        }
        fullLevelToMerge++;
        level = level->next;
    }

    int i;
    for(i = fullLevelToMerge; i > 0; i--){
        LSMLevel * levelToRead = getLevelAndCreateIfRequired(tree, i);
        //Latch the level to read from so that nothing can update it before the next level is written
        mergeLatchLevel(levelToRead);
        MergeInput * in = createMergeInput(tree, i);

        #ifdef THREADED_MERGES
        executeWithThreadOrDoItYourself(tree->threadPool, writeToLevelFromDisk, in);
        #endif // THREADED_MERGES

        #ifndef THREADED_MERGES
        writeToLevelFromDisk(in);
        #endif // THREADED_MERGES
    }

    MergeInput * in = createMergeInput(tree, 0);

    #ifdef THREADED_MERGES
    executeWithThreadOrDoItYourself(tree->threadPool, writeToC1, in);
    #endif // THREADED_MERGES

    #ifndef THREADED_MERGES
    writeToC1(in);
    #endif // THREADED_MERGES

    #ifdef EXPAND_DEBUG
    gettimeofday(&tv, NULL);
    unsigned long long millisecondsSinceEpochPost =
    (unsigned long long)(tv.tv_sec) * 1000 +
    (unsigned long long)(tv.tv_usec) / 1000;


    unsigned long long diff = millisecondsSinceEpochPost - millisecondsSinceEpochPre;
    printf("expand took: %llu\n", diff);

    #endif // EXPAND_DEBUG
}

void lsm_put(LSMTree * tree, int32_t key, int32_t value){
    readLatchC0onceReadsAreComplete(tree);
    readUnlatchC0(tree);
    if(!bt_insert(tree->activeC0, key, value)){
        expandTree(tree);
        bt_insert(tree->activeC0, key, value);
    }
}

static void printSummaryFromTree(BTree * tree){
    BTreeIterator * iter = bt_createIterator(tree);

    LSMData * data = bt_iterNext(iter);
    while(data != NULL){
        if(data->metadata != DELETE){
            printf("%i:%i:%s ", data->key, data->value,"L1");
        }
        data = bt_iterNext(iter);
    }

    bt_freeIterator(iter);

}

void lsm_printSummary(LSMTree * tree){

    //Wait for tree modifications to complete
    mergeLatchC0(tree);

    unsigned int overallTotal = 0;

    //Wait for all reads on c0 to complete
    readLatchC0onceReadsAreComplete(tree);
    readUnlatchC0(tree);

    unsigned int c0total = bt_getCount(tree->activeC0) + bt_getCount(tree->inactiveC0);

    overallTotal += c0total;

    LSMLevel * level = tree->levelsOnDisk;
    while(level != NULL){

        //Wait for all reads on this level to complete
        readLatchLevelOnceReadsAreComplete(level);
        readUnlatchLevel(level);

        ArrayOnDisk * diskArray = lvl_getLastDiskArray(level);
        while(diskArray != NULL){
            overallTotal += diskArray->size - diskArray->deleteCount;
            diskArray = diskArray->prev;
        }
        level = level->next;
    }

    printf("Total Pairs: %i\n", overallTotal);

	int printComma = 0;

	if(c0total){
        printf("LVL1: %i",c0total);
        printComma = 1;
	}

	//Get level counts
	level = tree->levelsOnDisk;
    while(level != NULL){
        ArrayOnDisk * diskArray = lvl_getLastDiskArray(level);
        int totalForLevel = 0;
        while(diskArray != NULL){
            totalForLevel += diskArray->size - diskArray->deleteCount;
            diskArray = diskArray->prev;
        }
        if(printComma++){
            printf(", ");
        }
        printf("LVL%u: %i", level->levelNumber + 1, totalForLevel);
        level = level->next;
    }

	printf("\n");

	printSummaryFromTree(tree->activeC0);
	printSummaryFromTree(tree->inactiveC0);

    printf("\n");

    level = tree->levelsOnDisk;
    while(level != NULL){
        ArrayOnDisk * diskArray = lvl_getLastDiskArray(level);
        char label[] = getLevelName(level->levelNumber + 1);
        while(diskArray != NULL){
            char filename[] = getFileName(level->levelNumber, diskArray->id);
            FILE * f = fopen(filename, "r");
            sa_printArrayFile(f, label);
            fclose(f);
            diskArray = diskArray->prev;

        }
        printf("\n");
        level = level->next;
    }

    mergeUnlatchC0(tree);
}

static void handleResultDuringSearch(LSMTree * tree,LSMData * result, unsigned long id){
    out_write(tree->buffer, result, id);
}

//Struct containing input for a search of levels on disk
typedef struct{
    LSMTree * tree;
    int32_t key;
    unsigned long id;
}SearchInput;


void * searchLevelsOnDisk(void * input){
    SearchInput * searchIn = (SearchInput *) input;
    int32_t key = searchIn->key;
    LSMLevel * level = searchIn->tree->levelsOnDisk;
    LSMData data;
    while(level != NULL){
        addRead(level);
        ArrayOnDisk * diskArray = lvl_getLastDiskArray(level);

        while(diskArray != NULL){

            if(!bf_contains(diskArray->filter, key)){
                diskArray = diskArray->prev;
                continue;
            }

            char filename[] = getFileName(level->levelNumber, diskArray->id);
            FILE * f = fopen(filename, "r");


            #ifdef USE_FENCE
            unsigned int address;
            if(fnc_findAddress(diskArray->fence,key,&address)){
                fseek(f,address,SEEK_SET);
            }
            #endif // USE_FENCE

            if(sa_searchForKeyInFile( f,key, &(data.value), &(data.metadata))){
                handleResultDuringSearch(searchIn->tree, &data, searchIn->id);

                fclose(f);
                free(searchIn);
                removeRead(level);
                return NULL;
            }
            fclose(f);
            //printf("==bloom filter false positive\n");

            diskArray = diskArray->prev;
        }

        LSMLevel * completeLevel = level;
        level = level->next;
        removeRead(completeLevel);
    }
    //not found
    data.metadata = NOT_FOUND;
    handleResultDuringSearch(searchIn->tree, &data, searchIn->id);
    free(searchIn);
    return NULL;
}

void lsm_getAndPrint(LSMTree * tree, int key){

    addC0Read(tree);

    unsigned long readID = at_addToCount(&(tree->buffer->writeID));

    LSMData * result = bt_search(tree->activeC0, key);

    if(result != NULL){
        handleResultDuringSearch(tree, result, readID);
        removeC0Read(tree);
        return;
    }

    result = bt_search(tree->inactiveC0, key);
    if(result != NULL){
        handleResultDuringSearch(tree, result, readID);
        removeC0Read(tree);
        return;
    }

    removeC0Read(tree);

    //Search lower levels
    SearchInput * in = malloc(sizeof(SearchInput));
    in->key = key;
    in->tree = tree;
    in->id = readID;

    #ifdef THREADED_READS
    executeWithThreadOrDoItYourself(tree->threadPool, searchLevelsOnDisk, in);
    #endif // THREADED_READS

    #ifndef THREADED_READS
    searchLevelsOnDisk(in);
    #endif // THREADED_READS

}

void lsm_free(LSMTree * tree){

    tp_free(tree->threadPool);

    bt_freeTree(tree->activeC0);
    bt_freeTree(tree->inactiveC0);

    LSMLevel * level = tree->levelsOnDisk;
    while(level != NULL){
        lvl_clearLevel(level);
        LSMLevel * rem = level;
        level = level->next;
        free(rem);
    }
    out_freeBuffer(tree->buffer);
    free(tree);
}

void lsm_delete(LSMTree * tree, int32_t key){
    readLatchC0onceReadsAreComplete(tree);
    readUnlatchC0(tree);
    if(!bt_delete(tree->activeC0, key)){
        expandTree(tree);
        bt_delete(tree->activeC0, key);
    }
}

//Input for a
typedef struct RangeResultHolderNode{
    RangeResult * result;
    bool complete;
    struct RangeResultHolderNode * next;
    LSMLevel * levelToScan;
    int32_t min;
    int32_t max;
}RangeResultInput;

//Precondition: The activeReadCount for the level has already been incremented
void * scanLevelForRange(void * input){
    RangeResultInput * rangeInput = (RangeResultInput * ) input;
    ArrayOnDisk * diskArray = lvl_getLastDiskArray(rangeInput->levelToScan);

    RangeResult * range = rangeInput->result;

    int32_t min = rangeInput->min;
    int32_t max = rangeInput->max;

    while(diskArray != NULL){

        char filename[] = getFileName(rangeInput->levelToScan->levelNumber, diskArray->id);
        FILE * f = fopen(filename, "r");

        #ifdef USE_FENCE
        unsigned int address;
        if(fnc_findAddress(diskArray->fence,min,&address)){
            fseek(f,address,SEEK_SET);
        }
        #endif // USE_FENCE

        sa_scanFileForRange(f, range, min,max);

        fclose(f);

        diskArray = diskArray->prev;
    }
    rangeInput->complete = true;
    removeRead(rangeInput->levelToScan);
    return NULL;
}

void lsm_printRange(LSMTree * tree, int32_t min, int32_t max){

    RangeResultInput * results = NULL;
    RangeResultInput * iter = NULL;

    LSMLevel * level = tree->levelsOnDisk;
    while(level != NULL){
        addRead(level);

        RangeResultInput * holder = malloc(sizeof(RangeResultInput));
        holder->complete = false;
        holder->levelToScan = level;
        holder->max = max;
        holder->min = min;
        holder->next = NULL;
        holder->result = rng_init();

        if(iter == NULL){
            results = holder;
        }else{
            iter->next = holder;
        }
        iter = holder;
        executeWithThreadOrDoItYourself(tree->threadPool, scanLevelForRange, holder);

        level = level->next;
    }

    RangeResult * range = rng_init();

    addC0Read(tree);
    bt_scanForRange(tree->activeC0, range, min, max);
    bt_scanForRange(tree->inactiveC0, range, min, max);
    removeC0Read(tree);

    iter = results;

    while(iter != NULL){
        while(!iter->complete){
            at_sleep();
        }
        rng_merge(range, iter->result);

        RangeResultInput * completeRange = iter;
        iter = iter->next;

        rng_free(completeRange->result);
        free(completeRange);
    }

    //Wait for our turn to output
    waitForAllWritesToBeComplete(tree->buffer);
    rng_print(range);
    rng_free(range);

}
