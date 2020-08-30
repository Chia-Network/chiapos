/* -------------------------------------------------------------------------------------------------------------------------- */
/*  pmergesort-tpool.inl                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  A Thread Pool Implementation is a courtesy of Sun Microsystems' "Multithreaded Programming Guide"                         */
/*  therefore (c) 2010, Oracle Corporation and/or its affiliates                                                              */
/*  as per http://docs.oracle.com/cd/E19253-01/816-5137/ggedn/index.html                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  Created by Cyril Murzin                                                                                                   */
/*  Copyright (c) 2016, 2017 Ravel Developers Group. All rights reserved.                                                     */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */

#include <pthread.h>
#include <signal.h>
#include <errno.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

static void clock_realtime(struct timespec * ts)
{
#ifdef __MACH__ /* OS X does not have clock_gettime, use clock_get_time */
    clock_serv_t cclock;
    mach_timespec_t mts;

    host_get_clock_service(mach_host_self(), REALTIME_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);

    ts->tv_sec = mts.tv_sec;
    ts->tv_nsec = mts.tv_nsec;
#else
    clock_gettime(CLOCK_REALTIME, ts);
#endif
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/*
 * FIFO queued job
 */
typedef struct _job job_t;
struct _job
{
    job_t *         job_next;               /* linked list of jobs */
    void *          (*job_func)(void *);    /* function to call */
    void *          job_arg;                /* its argument */
};

/*
 * List of active worker threads, linked through their stacks.
 */
typedef struct _active active_t;
struct _active
{
    active_t *      active_next;            /* linked list of threads */
    pthread_t       active_tid;             /* active thread id */
};

/*
 * The thread pool, opaque to the clients.
 */
struct thr_pool
{
    thr_pool_t *    pool_forw;              /* circular linked list */
    thr_pool_t *    pool_back;              /* of all thread pools */
    pthread_mutex_t pool_mutex;             /* protects the pool data */
    pthread_cond_t  pool_busycv;            /* synchronization in pool_queue */
    pthread_cond_t  pool_workcv;            /* synchronization with workers */
    pthread_cond_t  pool_waitcv;            /* synchronization in pool_wait() */
    active_t *      pool_active;            /* list of threads performing work */
    job_t *         pool_head;              /* head of FIFO job queue */
    job_t *         pool_tail;              /* tail of FIFO job queue */
    pthread_attr_t  pool_attr;              /* attributes of the workers */
    int             pool_flags;             /* see below */
    unsigned int    pool_linger;            /* seconds before idle workers exit */
    int             pool_minimum;           /* minimum number of worker threads */
    int             pool_maximum;           /* maximum number of worker threads */
    int             pool_nthreads;          /* current number of worker threads */
    int             pool_idle;              /* number of idle workers */
};

/* pool_flags */
#define POOL_WAIT       0x01                /* waiting in thr_pool_wait() */
#define POOL_DESTROY    0x02                /* pool is being destroyed */

/* the list of all created and not yet destroyed thread pools */
static thr_pool_t * thr_pools = NULL;

/* protects thr_pools */
static pthread_mutex_t thr_pool_lock = PTHREAD_MUTEX_INITIALIZER;

/* set of all signals */
static sigset_t fillset;

static void * worker_thread(void *);

static int create_worker(thr_pool_t *pool)
{
    sigset_t oset;
    int error;
    pthread_t tid;

    (void)pthread_sigmask(SIG_SETMASK, &fillset, &oset);
    error = pthread_create(&tid, &pool->pool_attr, worker_thread, pool);
    (void)pthread_sigmask(SIG_SETMASK, &oset, NULL);

    return error;
}

/*
 * Worker thread is terminating.  Possible reasons:
 * - excess idle thread is terminating because there is no work.
 * - thread was cancelled (pool is being destroyed).
 * - the job function called pthread_exit().
 * In the last case, create another worker thread
 * if necessary to keep the pool populated.
 */
static void worker_cleanup(thr_pool_t * pool)
{
    --pool->pool_nthreads;

    if ((pool->pool_flags & POOL_DESTROY) != 0)
    {
        if (pool->pool_nthreads == 0)
            (void) pthread_cond_broadcast(&pool->pool_busycv);
    }
    else if (pool->pool_head != NULL && pool->pool_nthreads < pool->pool_maximum && create_worker(pool) == 0)
    {
        pool->pool_nthreads++;
    }

    (void)pthread_mutex_unlock(&pool->pool_mutex);
}

static void notify_waiters(thr_pool_t * pool)
{
    if (pool->pool_head == NULL && pool->pool_active == NULL)
    {
        pool->pool_flags &= ~POOL_WAIT;

        (void)pthread_cond_broadcast(&pool->pool_waitcv);
    }
}

/*
 * Called by a worker thread on return from a job.
 */
static void job_cleanup(thr_pool_t * pool)
{
    pthread_t my_tid = pthread_self();
    active_t * activep;
    active_t ** activepp;

    (void)pthread_mutex_lock(&pool->pool_mutex);

    for (activepp = &pool->pool_active; (activep = *activepp) != NULL; activepp = &activep->active_next)
    {
        if (activep->active_tid == my_tid)
        {
            *activepp = activep->active_next;
            break;
        }
    }

    if ((pool->pool_flags & POOL_WAIT) != 0)
        notify_waiters(pool);
}

static void * worker_thread(void * arg)
{
    int timedout;
    job_t * job;
    void * (*func)(void *);
    active_t active;
    struct timespec ts;

    thr_pool_t * pool = (thr_pool_t *)arg;

    /*
     * This is the worker's main loop.  It will only be left
     * if a timeout occurs or if the pool is being destroyed.
     */
    (void)pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push((void (*)(void *))worker_cleanup, pool);

    active.active_tid = pthread_self();

    struct sched_param sp = { sched_get_priority_max(SCHED_RR) };
    (void)pthread_setschedparam(active.active_tid, SCHED_RR, &sp);

    for (;;)
    {
        /*
         * We don't know what this thread was doing during
         * its last job, so we reset its signal mask and
         * cancellation state back to the initial values.
         */
        (void)pthread_sigmask(SIG_SETMASK, &fillset, NULL);
        (void)pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
        (void)pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

        timedout = 0;
        pool->pool_idle++;
        if ((pool->pool_flags & POOL_WAIT) != 0)
            notify_waiters(pool);

        while (pool->pool_head == NULL && (pool->pool_flags & POOL_DESTROY) == 0)
        {
            if (pool->pool_nthreads <= pool->pool_minimum)
            {
                (void)pthread_cond_wait(&pool->pool_workcv, &pool->pool_mutex);
            }
            else
            {
                if (pool->pool_linger == 0)
                {
                    timedout = 1;
                    break;
                }

                clock_realtime(&ts);
                ts.tv_sec += pool->pool_linger;

                if (pthread_cond_timedwait(&pool->pool_workcv, &pool->pool_mutex, &ts) == ETIMEDOUT)
                {
                    timedout = 1;
                    break;
                }
            }
        }

        pool->pool_idle--;
        if ((pool->pool_flags & POOL_DESTROY) != 0)
            break;

        if ((job = pool->pool_head) != NULL)
        {
            timedout = 0;
            func = job->job_func;
            arg = job->job_arg;
            pool->pool_head = job->job_next;
            if (job == pool->pool_tail)
                pool->pool_tail = NULL;
            active.active_next = pool->pool_active;
            pool->pool_active = &active;
            (void) pthread_mutex_unlock(&pool->pool_mutex);
            pthread_cleanup_push((void (*)(void *))job_cleanup, pool);
            PMR_FREE(job);
            /*
             * Call the specified job function.
             */
            (void)func(arg);
            /*
             * If the job function calls pthread_exit(), the thread
             * calls job_cleanup(pool) and worker_cleanup(pool);
             * the integrity of the pool is thereby maintained.
             */
            pthread_cleanup_pop(1); /* job_cleanup(pool) */
        }

        if (timedout && pool->pool_nthreads > pool->pool_minimum)
        {
            /*
             * We timed out and there is no work to be done
             * and the number of workers exceeds the minimum.
             * Exit now to reduce the size of the pool.
             */
            break;
        }
    }

    pthread_cleanup_pop(1); /* worker_cleanup(pool) */

    return NULL;
}

static void clone_attributes(pthread_attr_t * new_attr, pthread_attr_t * old_attr)
{
    struct sched_param param;
    size_t size;
    int value;

    (void) pthread_attr_init(new_attr);

    if (old_attr != NULL)
    {
        (void)pthread_attr_getstacksize(old_attr, &size);
        (void)pthread_attr_setstacksize(new_attr, size);

        (void)pthread_attr_getscope(old_attr, &value);
        (void)pthread_attr_setscope(new_attr, value);

        (void)pthread_attr_getinheritsched(old_attr, &value);
        (void)pthread_attr_setinheritsched(new_attr, value);

        (void)pthread_attr_getschedpolicy(old_attr, &value);
        (void)pthread_attr_setschedpolicy(new_attr, value);

        (void)pthread_attr_getschedparam(old_attr, &param);
        (void)pthread_attr_setschedparam(new_attr, &param);

        (void)pthread_attr_getguardsize(old_attr, &size);
        (void)pthread_attr_setguardsize(new_attr, size);
    }

    /* make all pool threads be detached threads */
    (void)pthread_attr_setdetachstate(new_attr, PTHREAD_CREATE_DETACHED);
}

/*
 * Create a thread pool.
 *  min_threads:    the minimum number of threads kept in the pool,
 *                  always available to perform work requests.
 *  max_threads:    the maximum number of threads that can be
 *                  in the pool, performing work requests.
 *  linger:         the number of seconds excess idle worker threads
 *                  (greater than min_threads) linger before exiting.
 *  attr:           attributes of all worker threads (can be NULL);
 *                  can be destroyed after calling thr_pool_create().
 * On error, thr_pool_create() returns NULL with errno set to the error code.
 */
static thr_pool_t * thr_pool_create(int min_threads, int max_threads, unsigned int linger, pthread_attr_t * attr)
{
    thr_pool_t * pool;

    (void)sigfillset(&fillset);

    if (min_threads < 0 || min_threads > max_threads || max_threads < 1)
    {
        errno = EINVAL;
        return NULL;
    }

    if ((pool = PMR_MALLOC(sizeof(*pool))) == NULL)
    {
        errno = ENOMEM;
        return NULL;
    }

    (void)pthread_mutex_init(&pool->pool_mutex, NULL);
    (void)pthread_cond_init(&pool->pool_busycv, NULL);
    (void)pthread_cond_init(&pool->pool_workcv, NULL);
    (void)pthread_cond_init(&pool->pool_waitcv, NULL);

    pool->pool_active = NULL;
    pool->pool_head = NULL;
    pool->pool_tail = NULL;
    pool->pool_flags = 0;
    pool->pool_linger = linger;
    pool->pool_minimum = min_threads;
    pool->pool_maximum = max_threads;
    pool->pool_nthreads = 0;
    pool->pool_idle = 0;

    /*
     * We cannot just copy the attribute pointer.
     * We need to initialize a new pthread_attr_t structure using
     * the values from the caller-supplied attribute structure.
     * If the attribute pointer is NULL, we need to initialize
     * the new pthread_attr_t structure with default values.
     */
    clone_attributes(&pool->pool_attr, attr);

    /* insert into the global list of all thread pools */
    (void)pthread_mutex_lock(&thr_pool_lock);
    if (thr_pools == NULL)
    {
        pool->pool_forw = pool;
        pool->pool_back = pool;
        thr_pools = pool;
    }
    else
    {
        thr_pools->pool_back->pool_forw = pool;
        pool->pool_forw = thr_pools;
        pool->pool_back = thr_pools->pool_back;
        thr_pools->pool_back = pool;
    }
    (void) pthread_mutex_unlock(&thr_pool_lock);

    return pool;
}

/*
 * Enqueue a work request to the thread pool job queue.
 * If there are idle worker threads, awaken one to perform the job.
 * Else if the maximum number of workers has not been reached,
 * create a new worker thread to perform the job.
 * Else just return after adding the job to the queue;
 * an existing worker thread will perform the job when
 * it finishes the job it is currently performing.
 *
 * The job is performed as if a new detached thread were created for it:
 *      pthread_create(NULL, attr, void *(*func)(void *), void *arg);
 *
 * On error, thr_pool_queue() returns -1 with errno set to the error code.
 */
static int thr_pool_queue(thr_pool_t * pool, void * (*func)(void *), void * arg)
{
    job_t * job;

    if ((job = PMR_MALLOC(sizeof (*job))) == NULL)
    {
        errno = ENOMEM;
        return -1;
    }

    job->job_next = NULL;
    job->job_func = func;
    job->job_arg = arg;

    (void)pthread_mutex_lock(&pool->pool_mutex);

    if (pool->pool_head == NULL)
        pool->pool_head = job;
    else
        pool->pool_tail->job_next = job;
    pool->pool_tail = job;

    if (pool->pool_idle > 0)
        (void)pthread_cond_signal(&pool->pool_workcv);
    else if (pool->pool_nthreads < pool->pool_maximum && create_worker(pool) == 0)
        pool->pool_nthreads++;

    (void)pthread_mutex_unlock(&pool->pool_mutex);

    return 0;
}

/*
 * Wait for all queued jobs to complete.
 */
static void thr_pool_wait(thr_pool_t * pool)
{
    (void) pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push((void (*)(void *))pthread_mutex_unlock, &pool->pool_mutex);
    while (pool->pool_head != NULL || pool->pool_active != NULL)
    {
        pool->pool_flags |= POOL_WAIT;
        (void) pthread_cond_wait(&pool->pool_waitcv, &pool->pool_mutex);
    }
    pthread_cleanup_pop(1); /* pthread_mutex_unlock(&pool->pool_mutex); */
}

/*
 * Cancel all queued jobs and destroy the pool.
 */
static void thr_pool_destroy(thr_pool_t * pool)
{
    active_t * activep;
    job_t * job;

    (void)pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push((void (*)(void *))pthread_mutex_unlock, &pool->pool_mutex);

    /* mark the pool as being destroyed; wakeup idle workers */
    pool->pool_flags |= POOL_DESTROY;
    (void)pthread_cond_broadcast(&pool->pool_workcv);

    /* cancel all active workers */
    for (activep = pool->pool_active; activep != NULL; activep = activep->active_next)
        (void)pthread_cancel(activep->active_tid);

    /* wait for all active workers to finish */
    while (pool->pool_active != NULL)
    {
        pool->pool_flags |= POOL_WAIT;
        (void)pthread_cond_wait(&pool->pool_waitcv, &pool->pool_mutex);
    }

    /* the last worker to terminate will wake us up */
    while (pool->pool_nthreads != 0)
        (void)pthread_cond_wait(&pool->pool_busycv, &pool->pool_mutex);

    pthread_cleanup_pop(1); /* pthread_mutex_unlock(&pool->pool_mutex); */

    /*
     * Unlink the pool from the global list of all pools.
     */
    (void)pthread_mutex_lock(&thr_pool_lock);
    if (thr_pools == pool)
        thr_pools = pool->pool_forw;
    if (thr_pools == pool)
        thr_pools = NULL;
    else
    {
        pool->pool_back->pool_forw = pool->pool_forw;
        pool->pool_forw->pool_back = pool->pool_back;
    }
    (void)pthread_mutex_unlock(&thr_pool_lock);

    /*
     * There should be no pending jobs, but just in case...
     */
    for (job = pool->pool_head; job != NULL; job = pool->pool_head)
    {
        pool->pool_head = job->job_next;
        PMR_FREE(job);
    }

    (void)pthread_attr_destroy(&pool->pool_attr);

    PMR_FREE(pool);
}

/* -------------------------------------------------------------------------------------------------------------------------- */
