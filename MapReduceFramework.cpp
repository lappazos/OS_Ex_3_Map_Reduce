#include "MapReduceFramework.h"
#include "MultiThreadJob.h"

/**
 *This function produces a (K2*,V2*) pair
 * @param key
 * @param value
 * @param context ThreadContext pointer
 */
void emit2(K2 *key, V2 *value, void *context)
{
    auto *threadContext = (MapThreadContext *) context;
    IntermediatePair toAdd(key, value);
    mutexLock(threadContext->thread_outputs_mutex);
    threadContext->thread_outputs->push_back(toAdd);
    mutexUnlock(threadContext->thread_outputs_mutex);
}

/**
 *  This function produces a (K3*,V3*) pair
 * @param key
 * @param value
 * @param context ThreadContext pointer
 */
void emit3(K3 *key, V3 *value, void *context)
{
    auto *threadContext = (ThreadContext *) context;
    OutputPair toAdd(key, value);
    mutexLock(&threadContext->curr_job->accessOutputVecMutex);
    threadContext->curr_job->outputVec->push_back(toAdd);
    mutexUnlock(&threadContext->curr_job->accessOutputVecMutex);
}

/**
 * This function starts running the MapReduce algorithm (with several threads)
 * @param client The implementation of MapReduceClient class, in other words the task that the framework should run.
 * @param inputVec a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
 * @param outputVec a vector of type std::vector<std::pair<K3*, V3*>>, to which the output elements will be added before returning.
 * @param multiThreadLevel the number of worker threads to be used for running the algorithm.
 * @return Job Handle
 */
JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
                  int multiThreadLevel)
{
    auto *job = new MultiThreadJob(client, inputVec, outputVec, multiThreadLevel);
    job->init();
    return job;
}

/**
 * a function waits until job is finished.
 * @param job job handle returned by startMapReduceFramework
 */
void waitForJob(JobHandle job)
{
    auto *jobToWait = (MultiThreadJob *) job;
    jobToWait->waitToFinish();
}

/**
 * updates the state of the job into the given JobState struct.
 * @param job Job Handle
 * @param state Job State struct
 */
void getJobState(JobHandle job, JobState *state)
{
    auto *jobToWait = (MultiThreadJob *) job;
    uint64_t stage = (*(jobToWait->atomic_stage)).load();
    uint32_t total = (uint32_t) stage & (0x7fffffff);
    uint32_t processed = (uint32_t) (stage >> 31) & (0x7fffffff);
    state->percentage = (float(processed) / total) * 100;
    state->stage = static_cast<stage_t>(stage >> 62);
}

/**
 * Releasing all resources of a job. After this function is called the job handle will be invalid.
 * @param job job handle
 */
void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    auto *jobToClose = (MultiThreadJob *) job;
    delete jobToClose;
}

