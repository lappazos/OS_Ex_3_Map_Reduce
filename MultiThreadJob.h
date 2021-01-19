#include <pthread.h>
#include <cstdio>
#include <atomic>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"


#ifndef MULTITHREADJOB_H
#define MULTITHREADJOB_H

static const char *const SYS_ERROR = "system error";

/**
 * class to hols all resources needed for a successful multi-thread job run
 */
class MultiThreadJob;

struct ThreadContext
{
    MultiThreadJob *curr_job;
};

struct MapThreadContext : ThreadContext
{
    std::vector<IntermediatePair> *thread_outputs;
    pthread_mutex_t *thread_outputs_mutex;
};

/**
 * thread reduce stage function
 * @param context ThreadContext pointer
 */
void ReduceThreadMainWork(ThreadContext *context);

/**
 * map threads start function
 * @param args MapThreadContext pointer
 */
void *MapThreadMainWork(void *args);

/**
 * perform Shuffle stage work
 * @param threadContext ThreadContext pointer
 * @return number of elements inserted into the intermediateMap
 */
uint32_t empty_threads_outputs(const ThreadContext *threadContext);

/**
 * args shuffle thread start function
 * @param ThreadContext
 */
void *ShuffleThreadMainWork(void *args);


class MultiThreadJob
{
private:
    pthread_t *pool;
    int multiThreadLevel;
    MapThreadContext *mapThreadContexts;
    ThreadContext shuffleThreadContext;
    bool threadsJoined;

public:

    const InputVec* inputVec;
    int numOfMapThreads;
    const MapReduceClient *client;
    std::atomic<uint64_t>* atomic_stage;  // 2 first bit is for stage, (UNDEFINED, MAP, SHUFFLE,
    // REDUCE), 31 bit for already processed keys and 31 bit for total keys to process
    std::atomic<uint64_t>* atomic_inputVec_index;
    std::atomic<bool> *MapStageEnded;
    int map_finished_counter;
    OutputVec *outputVec;
    pthread_mutex_t waitShuffleEndMutex; // condition mutex
    pthread_mutex_t accessIntermediateVecMutex;
    pthread_mutex_t accessOutputVecMutex;
    pthread_cond_t cvWaitShuffleEnds;
    std::vector<IntermediatePair> *threads_outputs; //list of personal vector for each thread
    pthread_mutex_t *threads_outputs_mutex; //list of personal mutex for each thread
    IntermediateMap intermediateMap;

    /**
     * Constructor
     * @param client The implementation of MapReduceClient class, in other words the task that the framework should run.
     * @param inputVec a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
     * @param outputVec a vector of type std::vector<std::pair<K3*, V3*>>, to which the output elements will be added before returning.
     * @param multiThreadLevel the number of worker threads to be used for running the algorithm.
     */
    MultiThreadJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
                   int multiThreadLevel);

    /**
     * create the threads
     */
    void init();

    /**
     * wait for threads to finish
     */
    void waitToFinish();

    /**
     * destructor
     */
    ~MultiThreadJob();

};

/**
 * lock with error check
 * @param mutex
 */

void mutexLock(pthread_mutex_t *mutex);

/**
 * unlock with error check
 * @param mutex
 */
void mutexUnlock(pthread_mutex_t *mutex);

/**
 * destroy with error check
 * @param mutex
 */
void mutexDestroy(pthread_mutex_t *mutex);

/**
 * init with error check
 * @param mutex
 */
void mutexInit(pthread_mutex_t *mutex);

#endif //MULTITHREADJOB_H
