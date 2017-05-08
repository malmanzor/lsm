#include <pthread.h>
#include "value.h"

/*
    Module containing functionality for running functions with a thread pool if
    possible
*/

//A worker thread that lives in a thread pool and can be given work
typedef struct{
    pthread_t thread;
    bool hasWork;
    void *(*job) (void *);
    void * arg;
    bool shouldHalt;
    bool sleeping;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
}WorkerThread;

//A thread pool storing worker threads
typedef struct{
    int size;
    WorkerThread * threads;
    bool active;
    char latch;
}ThreadPool;

//Create a thread pool with the specified number of threads
ThreadPool * tp_create(int size);

//Execute a function with a given argument with a thread if there an available thread in the pool.
//If not, run the function in the current thread.
void executeWithThreadOrDoItYourself(ThreadPool * pool,  void *(*job) (void *), void * arg);

//Free a thread pool
void tp_free(ThreadPool * pool);

