#include "value.h"
#include <stdio.h>
#include <string.h>
#include "RangeResult.h"

RangeResult * rng_init(){
    RangeResult * result = malloc(sizeof(RangeResult));
    result->head = NULL;
    result->iter = NULL;
    return result;
}

static RangeResultInstance * initResultInstance(LSMData * value){
    RangeResultInstance * inst = malloc(sizeof(RangeResultInstance));
    memcpy(inst,value,sizeof(LSMData));
    inst->next = NULL;
    inst->prev = NULL;
    return inst;
};

void rng_add(RangeResult * range, LSMData * data){
    int key = data->key;

    if(range->head == NULL){
            range->iter = initResultInstance(data);
            range->head = range->iter;
            return;
    }

    if(range->iter == NULL || range->iter->value.key > data->key){
        range->iter = range->head;
    }

    while(range->iter != NULL){
        int currentValue = range->iter->value.key;
        if(currentValue == key){
            return; //Key has already been found
        }

        if(currentValue > key){
            //Insert the new data behind the iterator
            RangeResultInstance * newInst = initResultInstance(data);
            newInst->next = range->iter;
            if(range->iter->prev != NULL){
                range->iter->prev->next = newInst;
                newInst->prev = range->iter->prev;
            }else{
                range->head = newInst;
                newInst->prev = NULL;
            }

            range->iter->prev = newInst;
            return;
        }else if(range->iter->next == NULL){
            //Append to the list
            RangeResultInstance * newInst = initResultInstance(data);
            range->iter->next = newInst;
            newInst->prev = range->iter;
            range->iter = newInst;
            return;
        }
        range->iter = range->iter->next;
    }

}

void rng_print(RangeResult * range){
    RangeResultInstance * i = range->head;

    while(i != NULL){

        if(i->value.metadata != DELETE){
            printf("%i:%i ", i->value.key, i->value.value);
        }
        i = i->next;
    }
    printf("\n");
}

void rng_free(RangeResult * range){
    RangeResultInstance * inst = range->head;

    while(inst != NULL){
       RangeResultInstance * rem = inst;
       inst = inst->next;
       free(rem);
    }

    free(range);
}

void rng_merge(RangeResult * rangeToMergeInto, RangeResult * rangeToRead){

    RangeResultInstance * iter = rangeToRead->head;
    while(iter != NULL){
        rng_add(rangeToMergeInto, &(iter->value));
        iter = iter->next;
    }

}
