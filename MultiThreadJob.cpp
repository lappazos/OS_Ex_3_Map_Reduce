#include "MultiThreadJob.h"

void mutexLock(pthread_mutex_t *mutex)
{
    if (pthread_mutex_lock(mutex) != 0)
    {
        perror(SYS_ERROR);
        exit(1);
    }
}

void mutexUnlock(pthread_mutex_t *mutex)
{
    if (pthread_mutex_unlock(mutex) != 0)
    {
        perror(SYS_ERROR);
        exit(1);
    }
}

void mutexDestroy(pthread_mutex_t *mutex)
{
    if (pthread_mutex_destroy(mutex) != 0)
    {
        perror(SYS_ERROR);
        exit(1);
    }
}

void mutexInit(pthread_mutex_t *mutex)
{
    if (pthread_mutex_init(mutex, nullptr) != 0)
    {
        perror(SYS_ERROR);
        exit(1);
    }
}

void ReduceThreadMainWork(ThreadContext *threadContext)
{
    while (true)
    {
        mutexLock(&threadContext->curr_job->accessIntermediateVecMutex);
        if (threadContext->curr_job->intermediateMap.empty())
        {
            mutexUnlock(&threadContext->curr_job->accessIntermediateVecMutex);  // Todo - Ugly maybe change
            break;
        }
        auto it = threadContext->curr_job->intermediateMap.begin();
        K2* first = it->first;
        std::vector<V2*> second = it->second;
        threadContext->curr_job->intermediateMap.erase(it);
        mutexUnlock(&threadContext->curr_job->accessIntermediateVecMutex);
        threadContext->curr_job->client->reduce(first, second, threadContext);
        (*(threadContext->curr_job->atomic_stage)) += (((uint64_t)1) << 31);
    }
    pthread_exit(nullptr);
}

void *MapThreadMainWork(void *args)
{
    auto *threadContext = (MapThreadContext *) args;
    size_t old_index = 0;

    (*(threadContext->curr_job->atomic_stage)).fetch_or(
            (((uint64_t) 1) << 62) + (uint64_t) threadContext->curr_job->inputVec->size());

    while ((old_index = ((*(threadContext->curr_job->atomic_inputVec_index)) ++)) <
           threadContext->curr_job->inputVec->size())
    {
        const InputPair pair = threadContext->curr_job->inputVec->at(old_index);
        threadContext->curr_job->client->map(pair.first, pair.second, threadContext);
        (*(threadContext->curr_job->atomic_stage)) += ((uint64_t)1) << 31;
    }

    //wait for all map threads to finish
    mutexLock(&threadContext->curr_job->waitShuffleEndMutex);
    threadContext->curr_job->map_finished_counter -= 1;
    if (threadContext->curr_job->map_finished_counter > 0)
    {
        // map threads wait for shuffle to end
        if (pthread_cond_wait(&threadContext->curr_job->cvWaitShuffleEnds,
                              &threadContext->curr_job->waitShuffleEndMutex) != 0)
        {
            perror(SYS_ERROR);
            exit(1);
        }
    } else
    {
        (*(threadContext->curr_job->MapStageEnded)).store(true);
        if (pthread_cond_wait(&threadContext->curr_job->cvWaitShuffleEnds,
                              &threadContext->curr_job->waitShuffleEndMutex) != 0)
        {
            perror(SYS_ERROR);
            exit(1);
        }
    }
    mutexUnlock(&threadContext->curr_job->waitShuffleEndMutex);
    ReduceThreadMainWork(threadContext);
    return nullptr;
}

uint32_t empty_threads_outputs(const ThreadContext *threadContext, std::vector<IntermediatePair>* v)
{
    uint32_t overAllShuffleInserted = 0;
    for (int i = 0; i < threadContext->curr_job->numOfMapThreads; i ++)
    {
        if ((*(threadContext->curr_job->MapStageEnded)).load())
        {
            break;
        }
        pthread_mutex_t *thread_vector_mutex = threadContext->curr_job->threads_outputs_mutex + i;
        mutexLock(thread_vector_mutex);
        v->swap(threadContext->curr_job->threads_outputs[i]);
        mutexUnlock(thread_vector_mutex);
        if (!v->empty())
        {
            for (auto pair: *v)
            {
                threadContext->curr_job->intermediateMap[pair.first].push_back(pair.second);
            }
            overAllShuffleInserted += v->size();
            v->clear();
        }
    }
    return overAllShuffleInserted;
}

void *ShuffleThreadMainWork(void *args)
{
    auto *threadContext = (ThreadContext *) args;
    uint64_t overallShuffleInserted = 0;
    auto* v = new std::vector<IntermediatePair>();
    while (! (*(threadContext->curr_job->MapStageEnded)).load())
    {
        overallShuffleInserted += empty_threads_outputs(threadContext, v);
    }
    delete v;

    // move to shuffle state
    uint64_t overAllShuffleExist = overallShuffleInserted;
    for (int i = 0; i < threadContext->curr_job->numOfMapThreads; i ++)
    {
        std::vector<IntermediatePair> *thread_outputs = threadContext->curr_job->threads_outputs + i;
        overAllShuffleExist += thread_outputs->size();
    }
    (*(threadContext->curr_job->atomic_stage)).store((((uint64_t)2) << 62) + ((uint64_t)
    (overallShuffleInserted << 31)) + ((uint64_t) overAllShuffleExist));

    // Shuffle state process
    for (int i = 0; i < threadContext->curr_job->numOfMapThreads; i ++)
    {
        std::vector<IntermediatePair> *thread_outputs = threadContext->curr_job->threads_outputs + i;
        for (IntermediatePair pair:*thread_outputs)
        {
            threadContext->curr_job->intermediateMap[pair.first].push_back(pair.second);
            (*(threadContext->curr_job->atomic_stage)) += (((uint64_t)1) << 31);
        }
    }
    mutexLock(&threadContext->curr_job->waitShuffleEndMutex); // in case right after MapStageEnded
            // == true there was a switch before the last thread wait set
    if (pthread_cond_broadcast(&threadContext->curr_job->cvWaitShuffleEnds) != 0)
    {
        perror(SYS_ERROR);
        exit(1);
    }
    (*(threadContext->curr_job->atomic_stage)).store(
            (((uint64_t) 3) << 62) + (uint64_t) threadContext->curr_job->intermediateMap.size());
    mutexUnlock(&threadContext->curr_job->waitShuffleEndMutex);
    ReduceThreadMainWork(threadContext);
    return nullptr;
}

void MultiThreadJob::init()
{
    for (int i = 0; i < numOfMapThreads; i ++)
    {
        if (pthread_create(pool + i, nullptr, MapThreadMainWork, mapThreadContexts + i) != 0)
        {
            perror(SYS_ERROR);
            exit(1);
        }
    }
    if (pthread_create(pool + numOfMapThreads, nullptr, ShuffleThreadMainWork, &shuffleThreadContext) != 0)
    {
        perror(SYS_ERROR);
        exit(1);
    }
}

void MultiThreadJob::waitToFinish()
{
    if (!threadsJoined)
    {
        for (int i = 0; i < multiThreadLevel; i++)
        {
            if (pthread_join(pool[i], nullptr) != 0)
            {
                perror(SYS_ERROR);
                exit(1);
            }
        }
    }
    threadsJoined = true;
}


MultiThreadJob::~MultiThreadJob()
{
    delete[] pool;
    delete[] threads_outputs;
    for (int i = 0; i < numOfMapThreads; ++ i)
    {
        mutexDestroy(threads_outputs_mutex + i);
    }
    delete[] threads_outputs_mutex;
    delete atomic_stage;
    delete atomic_inputVec_index;
    delete MapStageEnded;
    mutexDestroy(&waitShuffleEndMutex);
    mutexDestroy(&accessIntermediateVecMutex);
    mutexDestroy(&accessOutputVecMutex);
    if (pthread_cond_destroy(&cvWaitShuffleEnds) != 0)
    {
        perror(SYS_ERROR);
        exit(1);
    }
    delete[] mapThreadContexts;
}


MultiThreadJob::MultiThreadJob(const MapReduceClient &client, const InputVec &inputVec,
                               OutputVec &outputVec, int multiThreadLevel)
{
    threadsJoined = false;
    atomic_stage = new std::atomic<uint64_t>(0);
    atomic_inputVec_index = new std::atomic<uint64_t>(0);
    this->MapStageEnded = new std::atomic<bool>(false); //todo check return value system call
    // error
    numOfMapThreads = multiThreadLevel - 1;
    this->multiThreadLevel = multiThreadLevel;
    map_finished_counter = numOfMapThreads;
    pool = new pthread_t[multiThreadLevel];
    threads_outputs = new std::vector<IntermediatePair>[numOfMapThreads];
    threads_outputs_mutex = new pthread_mutex_t[numOfMapThreads];
    this->inputVec = &inputVec;  // todo: probably not good
    this->outputVec = &outputVec;
    this->mapThreadContexts = new MapThreadContext[numOfMapThreads];
    this->client = &client;

    for (int i = 0; i < numOfMapThreads; ++ i)
    {
        this->mapThreadContexts[i].thread_outputs = threads_outputs + i;
        mutexInit(threads_outputs_mutex + i);
        this->mapThreadContexts[i].thread_outputs_mutex = threads_outputs_mutex + i;
        this->mapThreadContexts[i].curr_job = this;
    }
    this->shuffleThreadContext.curr_job = this;
    mutexInit(&waitShuffleEndMutex);
    mutexInit(&accessIntermediateVecMutex);
    mutexInit(&accessOutputVecMutex);
    pthread_cond_init(&cvWaitShuffleEnds, nullptr);
}
