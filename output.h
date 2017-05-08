#include "value.h"
#include "atomics.h"

/*
    Module for synchronizing the printing of the result of read queries as they
    can be process asynchronously
*/

//A piece of data that has not been printed yet
typedef struct PendingWriteNode{
    unsigned long id;
    LSMData data;
} PendingWrite;

//An output buffer
typedef struct{
    unsigned long nextWrite;
    char latch;
    PendingWrite * writes;
    unsigned int writeIndex;
    AtomicCount writeID;
} OutputBuffer;

//Create an output buffer
OutputBuffer * out_initBuffer();

//Free a buffer
void out_freeBuffer(OutputBuffer * buffer);

//Write data to the buffer
void out_write(OutputBuffer * buffer,LSMData * dataToWrite, unsigned long writeID);

//Wait for all data to be flushed
void waitForAllWritesToBeComplete(OutputBuffer * buffer);
