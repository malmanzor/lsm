#include <stdatomic.h>
#include <time.h>
#include "atomics.h"

//How to sleep for when waiting on a resource
const struct timespec SLEEP_TIME = {0, 1};

void __attribute__((always_inline)) inline at_sleep(){
    nanosleep(&SLEEP_TIME, NULL);
}

unsigned long at_addToCount(AtomicCount * atomic){
    at_latchChar(&(atomic->latch));
    unsigned long val = ++atomic->count;
    at_unlatchChar(&(atomic->latch));
    return val;
}

unsigned long at_decrementCount(AtomicCount * atomic){
    at_latchChar(&(atomic->latch));
    unsigned long val = --atomic->count;
    at_unlatchChar(&(atomic->latch));
    return val;
}

void at_waitForZeroAndLatch(AtomicCount * atomic){
    while(1){
        if(atomic->count == 0){
            at_latchChar(&(atomic->latch));

            if(atomic->count == 0){
                return;
            }
            at_unlatchChar(&(atomic->latch));
        }
        at_sleep();
    }
}

void at_latchChar(volatile char * l){
    char unlocked = UNLOCKED;
    while(!atomic_compare_exchange_strong(l, &unlocked, LOCKED)){
        at_sleep();
        unlocked = UNLOCKED;
    }
}

void at_latchCharAndPrintDebug(volatile char * l){
    char unlocked = UNLOCKED;
    while(!atomic_compare_exchange_strong(l, &unlocked, LOCKED)){

        #ifdef LSM_DEBUG
        printf("waiting for latch\n");
        #endif // LSM_DEBUG

        at_sleep();
        unlocked = UNLOCKED;
    }
}

void at_unlatchChar(volatile char * l){
    *l = UNLOCKED;
}

void at_initAtomicCount(AtomicCount * atomic){
    atomic->count = 0;
    atomic->latch = UNLOCKED;
}


