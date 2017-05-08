#include "threadpool.h"
#include <pthread.h>
#include <stdlib.h>
#include "atomics.h"

void * processTasks(void * arg){
    WorkerThread * thread = (WorkerThread *) arg;

    for(;;){
        thread->sleeping = false;
        if(thread->hasWork){
            thread->job(thread->arg);
            thread->hasWork = false;
        }else if(thread->shouldHalt){
            return NULL;
        }else{
            pthread_mutex_lock(&thread->mutex);
            thread->sleeping = true;
            pthread_cond_wait(&thread->condition, &thread->mutex);
            pthread_mutex_unlock(&thread->mutex);
        }
    }
}

ThreadPool * tp_create(int size){
    ThreadPool * pool = malloc(sizeof(ThreadPool));
    pool->size = size;
    pool->threads = malloc(sizeof(WorkerThread) * size);
    pool->latch = UNLOCKED;
    pool->active = true;
    int i;

    for(i = 0; i < size; i++){
        WorkerThread * t = &(pool->threads[i]);
        t->arg = NULL;
        t->hasWork = false;
        t->job = NULL;
        t->shouldHalt = false;
        t->sleeping = false;
        pthread_mutex_init(&t->mutex, NULL);
        pthread_cond_init(&t->condition, NULL);
        pthread_create(&(t->thread), NULL, &processTasks, t);
    }
    return pool;
}

void executeWithThreadOrDoItYourself(ThreadPool * pool,  void *(*job) (void *), void * arg){

    if(pool->active){
        at_latchChar(&(pool->latch));

        int i;
        for(i = 0; i < pool->size; i++){
            WorkerThread * thread = &(pool->threads[i]);
            if(!thread->hasWork){
                pthread_mutex_lock(&thread->mutex);
                if(!thread->sleeping){
                    pthread_mutex_unlock(&thread->mutex);
                    continue;
                }
                thread->job = job;
                thread->arg = arg;
                thread->hasWork = true;
                pthread_cond_signal(&thread->condition);
                pthread_mutex_unlock(&thread->mutex);
                at_unlatchChar(&(pool->latch));
                return;
            }
        }

        at_unlatchChar(&(pool->latch));
    }
    job(arg);
}

void tp_free(ThreadPool * pool){
    pool->active = false;
    at_latchChar(&(pool->latch));

    void * returnVal;
    int i;
    for(i = 0; i < pool->size; i++){
        WorkerThread * thread = &(pool->threads[i]);
        thread->shouldHalt = true;

        pthread_mutex_lock(&thread->mutex);
        pthread_cond_signal(&thread->condition);
        pthread_mutex_unlock(&thread->mutex);

        pthread_join(thread->thread, &returnVal);
    }

    at_unlatchChar(&(pool->latch));

    free(pool->threads);
    free(pool);
}
