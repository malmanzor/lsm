#pragma once
#define LOCKED '0'
#define UNLOCKED '1'

/*
    Module containing functions for thread save operations


*/

//A long value that can be updated atomically and latched
//to prevent other threads from updating it
typedef struct{
    volatile char latch;
    unsigned long count;
}AtomicCount;

//Increment an atomic count by 1
unsigned long at_addToCount(AtomicCount * atomic);

//Decrement an atomic count by 1
unsigned long at_decrementCount(AtomicCount * atomic);

//Wait for a count to have a value of 0 and latch it
void at_waitForZeroAndLatch(AtomicCount * atomic);

//Change a char from UNLOCKED to LOCKED
//If the char is in state LOCKED then block until it is in the
//UNLOCKED state
void at_latchChar(volatile char * l);

//Change a char to UNLOCKED
//This function does not check the current value
//of the char and should only be used when a thread
//already has the char latched
void at_unlatchChar(volatile char * l);

//Pause the thread
void at_sleep();

//Initialize an atomic count instance
void at_initAtomicCount(AtomicCount * atomic);
