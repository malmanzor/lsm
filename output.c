#include "output.h"
#include "atomics.h"
#include "value.h"
#include <stdio.h>

#define OUTPUT_BUFFER_SIZE 1024

#define latchBuffer(buff) at_latchChar(&(buffer->latch))

#define unlatchBuffer(buff) at_unlatchChar(&(buffer->latch))



OutputBuffer * out_initBuffer(){
    OutputBuffer * buffer = malloc(sizeof(OutputBuffer));
    buffer->latch = UNLOCKED;
    buffer->nextWrite = 1;
    buffer->writes = calloc(OUTPUT_BUFFER_SIZE, sizeof(PendingWrite));
    buffer->writeIndex = 0;
    at_initAtomicCount(&(buffer->writeID));
    return buffer;
}

void out_freeBuffer(OutputBuffer * buffer){
    free(buffer->writes);
    free(buffer);
}

static __attribute__((always_inline)) inline void printDataAndIncrementID(OutputBuffer * buffer, LSMData * data){
    buffer->nextWrite++;
    if(data->metadata == DELETE || data->metadata == NOT_FOUND){
        printf("\n");
    }else{
        printf("%i\n", data->value);
    }
}

void waitForBufferToHaveSpaceOrMyTurnAndLatch(OutputBuffer * buffer, unsigned long writeID){
    while(1){
        if(buffer->writeIndex < OUTPUT_BUFFER_SIZE){
            latchBuffer(buffer);
            if(buffer->writeIndex < OUTPUT_BUFFER_SIZE){
                return;
            }
            unlatchBuffer(buffer);
        }else if(buffer->nextWrite == writeID){
            latchBuffer(buffer);
            return;
        }
        at_sleep();
    }
}

static void flushPossibleWrites(OutputBuffer * buffer){
    bool found = true;
    while(found){
        found = false;
        int index = buffer->writeIndex;
        int i;
        for(i = 0; i < index; i++){
            PendingWrite * write = &(buffer->writes[i]);
            if(buffer->nextWrite == write->id){
                printDataAndIncrementID(buffer, &(write->data));
                write->id = 0;

                while(buffer->writeIndex > 0 && buffer->writes[buffer->writeIndex - 1].id == 0){
                    buffer->writeIndex--;
                }

                found = true;
                break;
            }
        }
    }
}

static void writeNextData(OutputBuffer * buffer, LSMData * dataToWrite){
    printDataAndIncrementID(buffer, dataToWrite);
    flushPossibleWrites(buffer);
}

static void addToBuffer(OutputBuffer * buffer, LSMData * dataToWrite, unsigned long writeID){

    //Find a spot in the buffer
    int i;
    for(i = 0; i < buffer->writeIndex; i++){
        PendingWrite * spaceInBuffer =  &(buffer->writes[i]);
        if(spaceInBuffer->id == 0){
            spaceInBuffer->data = *dataToWrite;
            spaceInBuffer->id = writeID;
            return;
        }
    }

    //Append to the end of the buffer
    PendingWrite * newWrite =  &(buffer->writes[buffer->writeIndex++]);
    newWrite->data = *dataToWrite;
    newWrite->id = writeID;

}

void out_write(OutputBuffer * buffer,LSMData * dataToWrite, unsigned long writeID){
    latchBuffer(buffer);

    if(writeID == buffer->nextWrite){
        writeNextData(buffer, dataToWrite);
    }else{
        if(buffer->writeIndex == OUTPUT_BUFFER_SIZE){
            unlatchBuffer(buffer);
            waitForBufferToHaveSpaceOrMyTurnAndLatch(buffer, writeID);
        }

        if(writeID == buffer->nextWrite){
            writeNextData(buffer, dataToWrite);
        }else{
            addToBuffer(buffer, dataToWrite, writeID);
        }
    }

    unlatchBuffer(buffer);
}

void waitForAllWritesToBeComplete(OutputBuffer * buffer){
    while(buffer->nextWrite != buffer->writeID.count + 1){
        at_sleep();
    }
}
