/* -------------------------------------------------------------------------------------------------------------------------- */
/*  pmergesort.c                                                                                                              */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  Created by Cyril Murzin                                                                                                   */
/*  Copyright (c) 2015-2017 Ravel Developers Group. All rights reserved.                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "pmergesort.h"

/* -------------------------------------------------------------------------------------------------------------------------- */
/* configure build                                                                                                            */
/* -------------------------------------------------------------------------------------------------------------------------- */

#ifndef PMR_PARALLEL_USE_GCD
#define PMR_PARALLEL_USE_GCD        0   /* enable build of parallel merge sort algorithms, use GCD */
#endif

#ifndef PMR_PARALLEL_USE_PTHREADS
#define PMR_PARALLEL_USE_PTHREADS   1   /* enable build of parallel merge sort algorithms, use pthreads based pool */
#endif

#ifndef PMR_PARALLEL_USE_OMP
#define PMR_PARALLEL_USE_OMP        0   /* enable build of parallel merge sort algorithms, use OpenMP */
#endif

#ifndef PMR_RAW_ACCESS
#define PMR_RAW_ACCESS              1   /* enable raw memory access, 0 implies the using of memmove & memcpy */
#endif

#ifndef PMR_RAW_ACCESS_ALIGNED
#define PMR_RAW_ACCESS_ALIGNED      0   /* enable aligned raw memory access */
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
/* atomic memory function                                                                                                     */
/* -------------------------------------------------------------------------------------------------------------------------- */

#ifndef PMR_MEMCPY
#define PMR_MEMCPY(d,s,n)           memcpy((d),(s),(n))
#endif

#ifndef PMR_MEMMOVE
#define PMR_MEMMOVE(d,s,n)          memmove((d),(s),(n))
#endif

#ifndef PMR_MALLOC
#define PMR_MALLOC(s)               malloc((s))
#endif

#ifndef PMR_REALLOC
#define PMR_REALLOC(p,s)            realloc((p),(s))
#endif

#ifndef PMR_FREE
#define PMR_FREE(p)                 free((p))
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
/* Mac OS X & platform specific                                                                                               */
/* -------------------------------------------------------------------------------------------------------------------------- */

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#elif PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS || PMR_PARALLEL_USE_OMP
/* sentinel (temporary): reset to pthreads any attempt to compile parallel */
#   undef  PMR_PARALLEL_USE_GCD
#   define PMR_PARALLEL_USE_GCD         0
#   undef  PMR_PARALLEL_USE_PTHREADS
#   define PMR_PARALLEL_USE_PTHREADS    1
#   undef  PMR_PARALLEL_USE_OMP
#   define PMR_PARALLEL_USE_OMP         0
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
/* parallel fine tunings                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

#if PMR_PARALLEL_USE_GCD && !PMR_PARALLEL_USE_PTHREADS && !PMR_PARALLEL_USE_OMP
/*  parallel, GCD, assume Mac OS X  */
#   if !defined(MAC_OS_X_VERSION_10_6) || MAC_OS_X_VERSION_MIN_REQUIRED < MAC_OS_X_VERSION_10_6
#   error define PMR_PARALLEL_USE_PTHREADS to use parallel sort with pre-Mac OS X 10.6
#   endif
#elif !PMR_PARALLEL_USE_GCD && PMR_PARALLEL_USE_PTHREADS && !PMR_PARALLEL_USE_OMP
/*  parallel, pthreads  */
#elif !PMR_PARALLEL_USE_GCD && !PMR_PARALLEL_USE_PTHREADS && PMR_PARALLEL_USE_OMP
/*  parallel, OpenMP (if applicable)  */
#elif !PMR_PARALLEL_USE_GCD && !PMR_PARALLEL_USE_PTHREADS && !PMR_PARALLEL_USE_OMP
/*  single-threaded  */
#else
#   error PMR_PARALLEL_USE_* misconfiguration
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
/* fine tunings                                                                                                               */
/* -------------------------------------------------------------------------------------------------------------------------- */

#ifndef _PMR_CORE_PROFILE
#define _PMR_CORE_PROFILE           0   /* used for profiling */
#endif

#ifndef _PMR_QUEUE_OVERCOMMIT
#define _PMR_QUEUE_OVERCOMMIT       0   /* use private GCD queue attribute to force number of threads,
                                            see Apple Co. CoreFoundation source */
#endif

#ifndef _PMR_GCD_OVERCOMMIT
#define _PMR_GCD_OVERCOMMIT         0   /* allow overcommit GCD queue beyond of the number CPU cores */
#endif

#ifndef _PMR_PARALLEL_MAY_SPAWN
#define _PMR_PARALLEL_MAY_SPAWN     (PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS || PMR_PARALLEL_USE_OMP)
                                                    /* allow [sym]merge to spawn nested threads */
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
/* some more useless fine tunings */
/* -------------------------------------------------------------------------------------------------------------------------- */

#define _PMR_PRESORT                binsort_run     /* method of pre-sort for initial subsegments,
                                                        allowed: binsort, binsort_run, and binsort_mergerun */

#define _PMR_USE_4_MEM              PMR_RAW_ACCESS /* use dedicated int32 type memory ops */
#define _PMR_USE_8_MEM              PMR_RAW_ACCESS /* use dedicated int64 type memory ops */
#define _PMR_USE_16_MEM             PMR_RAW_ACCESS /* use dedicated int128 type memory ops */

#define _PMR_TMP_ROT                8   /* max. temp. elements at stack on rotate */

#define _PMR_MIN_SUBMERGELEN1       8   /* threshold to fallback from inplace [sym]merge to inplace merge
                                            for short left/right segment */
#define _PMR_MIN_SUBMERGELEN2       4   /* threshold to fallback from binary to linear search
                                            for short left/right segment merging */

#define _PMR_BLOCKLEN_MTHRESHOLD0   16
#define _PMR_BLOCKLEN_MTHRESHOLD    16
#define _PMR_BLOCKLEN_SYMMERGE      32  /* 20 was as in built-in GO language function */
#define _PMR_BLOCKLEN_MERGE         32

/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */

typedef struct thr_pool thr_pool_t;

#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS || PMR_PARALLEL_USE_OMP

#ifdef __APPLE__
#include <sys/sysctl.h>
#elif __hpux
#include <sys/mpctl.h>
#elif __sgi
#include <unistd.h>
#elif _WIN32
#   error not yet
#else
#include <unistd.h>
#endif

static int32_t _ncpu = -1;

#if _PMR_CORE_PROFILE
/*
 * override number of CPU for benchmark purposes
 * (may have sense for GCD at the moment)
 */
void pmergesort_nCPU(int32_t ncpu)
{
    _ncpu = ncpu;
}
#endif

#if PMR_PARALLEL_USE_PTHREADS
#define _PMR_ONCE_ARG   void
#elif PMR_PARALLEL_USE_GCD
#define _PMR_ONCE_ARG   void * ctx
#endif

/*
 * psort.c qotd:
 *
 * The turnoff value is the size of a partition that,
 * below which, we stop doing in parallel, and just do
 * in the current thread.  The value of sqrt(n) was
 * determined heuristically.  There is a smaller
 * dependence on the slowness of the comparison
 * function, and there might be a dependence on the
 * number of processors, but the algorithm has not been
 * determined.  Because the sensitivity to the turnoff
 * value is relatively low, we use a fast, approximate
 * integer square root routine that is good enough for
 * this purpose.
 *
 * CM: actually it depends on thread pool architecture as well
 *
 */

int wjbflsl(size_t n) {
    return (sizeof(size_t)*8) - __builtin_clzll(n);
/*
    int i = 0;
    while(n)
    {
	i++;
	if(n&1)
            break;
        n=n>>1;
    }
    return i;
*/
}

static __attribute__((noinline)) size_t cutOff(size_t n)
{
    size_t s = 1L << (wjbflsl(n) >> 1); // wjb flsl
    s = (s + n / s) >> 1;
    s = (s + n / s) >> 1;

#if PMR_PARALLEL_USE_PTHREADS
    return s << 4;
#elif PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_OMP
    return s << 2;
#endif
}

#if !PMR_PARALLEL_USE_OMP
static void __numCPU_initialize(_PMR_ONCE_ARG)
{
#ifdef __APPLE__
    int32_t mib[] = { CTL_HW, HW_AVAILCPU };

    size_t sz = sizeof(_ncpu);
    if (sysctl(mib, sizeof(mib) / sizeof(mib[0]), &_ncpu, &sz, NULL, 0) != 0)
        _ncpu = 1;
    else if (_ncpu <= 0)
        _ncpu = 1;
#elif __hpux
    int ncpu = mpctl(MPC_GETNUMSPUS, NULL, NULL);
    if (ncpu <= 0)
        _ncpu = 1;
    else
        _ncpu = (int32_t)ncpu;
#elif __sgi
    long ncpu = sysconf(_SC_NPROC_ONLN);
    if (ncpu <= 0)
        _ncpu = 1;
    else
        _ncpu = (int32_t)ncpu;
#else
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu <= 0)
        _ncpu = 1;
    else
        _ncpu = (int32_t)ncpu;
#endif
}
#endif /* PMR_PARALLEL_USE_OMP */

/* -------------------------------------------------------------------------------------------------------------------------- */
#if PMR_PARALLEL_USE_PTHREADS
/* -------------------------------------------------------------------------------------------------------------------------- */
#include "pmergesort-tpool.inl"
/* -------------------------------------------------------------------------------------------------------------------------- */

static __attribute__((noinline)) int numCPU()
{
static pthread_once_t _once = PTHREAD_ONCE_INIT;

    if (_ncpu <= 0)
        pthread_once(&_once, __numCPU_initialize);

    return (int)_ncpu;
}

/* -------------------------------------------------------------------------------------------------------------------------- */

static pthread_key_t _sKey = 0;

static __attribute__((noinline)) void __thPool_finalize(void * value)
{
    thr_pool_destroy((thr_pool_t *)value);
}

static __attribute__((noinline)) void __thPoolKey_initialize()
{
    pthread_key_create(&_sKey, __thPool_finalize);
}

static __attribute__((noinline)) thr_pool_t * thPool()
{
static pthread_once_t _once = PTHREAD_ONCE_INIT;

    if (_sKey == 0)
        pthread_once(&_once, __thPoolKey_initialize);

    thr_pool_t * pool = (thr_pool_t *)pthread_getspecific(_sKey);
    if (pool == NULL)
    {
        /*
         *  we have to create pool with some limits on simultaneously running
         *  threads. presuambly, for better performance, there shouldn't be
         *  more threads than number of CPU cores.
         */
        pool = thr_pool_create(numCPU() / 2, numCPU(), 1, NULL);

        pthread_setspecific(_sKey, pool);
    }

    return pool;
}

/* -------------------------------------------------------------------------------------------------------------------------- */
#elif PMR_PARALLEL_USE_GCD
/* -------------------------------------------------------------------------------------------------------------------------- */

#ifdef __INTEL_COMPILER
// Intel compiler __builtin_assume "hack"
#define __builtin_assume(c) __assume(c)
#endif

#include <dispatch/dispatch.h>
/* -------------------------------------------------------------------------------------------------------------------------- */

struct thr_pool
{
    dispatch_queue_t        queue;
#if _PMR_PARALLEL_MAY_SPAWN
    dispatch_group_t        group;
#if !_PMR_GCD_OVERCOMMIT
    dispatch_semaphore_t    mutex; /* semaphore to prevent the threads overcommit flood */
#endif
#endif
};

#ifdef __PGI
// PGI compiler __attribute__((__transparent_union__)) "hack"
#define DISPATCH_OBJECT_T(o) ((dispatch_object_t){ (void *)(o) })
#else
#define DISPATCH_OBJECT_T(o) (o)
#endif

#if _PMR_QUEUE_OVERCOMMIT
/*!
 * @enum dispatch_queue_flags_t
 *
 * @constant DISPATCH_QUEUE_OVERCOMMIT
 * The queue will create a new thread for invoking blocks, regardless of how
 * busy the computer is.
 */
enum
{
    DISPATCH_QUEUE_OVERCOMMIT = 0x2ULL
};

#define _PMR_DISPATCH_QUEUE_FLAGS   DISPATCH_QUEUE_OVERCOMMIT
#else
#define _PMR_DISPATCH_QUEUE_FLAGS   0
#endif
/* -------------------------------------------------------------------------------------------------------------------------- */

static __attribute__((noinline)) int numCPU()
{
static dispatch_once_t _once;

    if (_ncpu <= 0)
        dispatch_once_f(&_once, NULL, __numCPU_initialize);

    return (int)_ncpu;
}

/* -------------------------------------------------------------------------------------------------------------------------- */
#define thPool()    ((thr_pool_t *)0)
/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */
#elif PMR_PARALLEL_USE_OMP
/* -------------------------------------------------------------------------------------------------------------------------- */
#include <omp.h>
/* -------------------------------------------------------------------------------------------------------------------------- */

struct thr_pool
{
    size_t  ncpu;
};

/* -------------------------------------------------------------------------------------------------------------------------- */

static inline int numCPU()
{
    if (_ncpu <= 0)
        _ncpu = omp_get_num_procs();

    return (int)_ncpu;
}

/* -------------------------------------------------------------------------------------------------------------------------- */
#define thPool()    ((thr_pool_t *)0)
/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */
#endif /* PMR_PARALLEL_USE_PTHREADS */
/* -------------------------------------------------------------------------------------------------------------------------- */

#else /* PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS || PMR_PARALLEL_USE_OMP */

#if _PMR_CORE_PROFILE
void pmergesort_nCPU(int32_t ncpu)
{
    /* stub */
}
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
#define numCPU()    (0)
#define thPool()    ((thr_pool_t *)0)
#define cutOff(n)   (0)
/* -------------------------------------------------------------------------------------------------------------------------- */

#endif /* PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS */

/* -------------------------------------------------------------------------------------------------------------------------- */

typedef int (*cmpv_t)(const void *, const void *);
typedef int (*cmpr_t)(void *, const void *, const void *);

typedef int (*sort_t)(void * base, size_t n, size_t sz, cmpv_t cmp);
typedef int (*sort_r_t)(void * base, size_t n, size_t sz, void * thunk, cmpr_t cmp);

struct _context;
struct _aux
{
    int             rc;         /* result code of effector operation */ /* FIXME: atomic */
    struct _aux *   parent;     /* parent aux (initial one) for spawned threads, or 'self' for top */

    size_t          sz;         /* size of temp. buffer */
    void *          temp;       /* temp. buffer storage */
};
typedef struct _aux aux_t;

typedef void (*effector_t)(void * lo, void * mi, void * hi, struct _context * ctx, aux_t * aux);

/* -------------------------------------------------------------------------------------------------------------------------- */

struct _context
{
    /* sort context */

    const void *    base;           /* base address of array to sort    */
    const size_t    n;              /* number of elements               */
    const size_t    sz;             /* size of element                  */

    const void *    cmp;            /* comparator function              */
    const void *    thunk;          /* comparator thunk                 */

    /* parallel */

    const int       ncpu;           /* number of CPU cores              */
    thr_pool_t *    thpool;         /* thread pool (for pthread model)  */

    /* [sym]merge parallel */

    size_t          npercpu;        /* number of elements per CPU       */
    size_t          bsize;          /* initial block size               */
    size_t          cut_off;        /* min. len of subsegment to spawn  */
    effector_t      sort_effector;  /* effector to pre-sort blocks      */
    effector_t      merge_effector; /* effector to merge blocks         */

    /* [sym]merge parallel wrapper */

    const void *    wsort;          /* sort function to wrap            */
};
typedef struct _context context_t;

#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS
struct _pmergesort_pass_context
{
    context_t *     ctx;

    size_t          bsz;
    size_t          dbl_bsz;

    size_t          chunksz;
    size_t          numchunks;
#if PMR_PARALLEL_USE_PTHREADS
    size_t          chunk;      /* index of chunk (for pthread model)   */
#endif

    void *          lo;
    void *          mi;
    void *          hi;

    aux_t *         auxes;      /* array of per-thread aux data         */
    effector_t      effector;   /* pass effector (pre-sort or merge)    */
};
typedef struct _pmergesort_pass_context pmergesort_pass_context_t;
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/* memory accessors                                                                                                           */
/* -------------------------------------------------------------------------------------------------------------------------- */

#if PMR_RAW_ACCESS
#if __LP64__
#define T_WORD  uint64_t
#else
#define T_WORD  uint32_t
#endif

#if PMR_RAW_ACCESS_ALIGNED
#define T_MASK  (sizeof(T_WORD) - 1)
#endif

union _ptr
{
    T_WORD *    word;
    uint8_t *   byte;
    uintptr_t   uint;
};
typedef union _ptr ptr_t;
#endif /* !PMR_RAW_ACCESS */

static inline void _regions_swap(void * a, void * b, size_t sz)
{
#if PMR_RAW_ACCESS

    ptr_t p = { a };
    ptr_t q = { b };

#if PMR_RAW_ACCESS_ALIGNED
    size_t hsz = p.uint & T_MASK;
    if (hsz == (q.uint & T_MASK))
#endif
    {
        /* regions aligned */

        /* head */

        uint8_t tbyte;

#if PMR_RAW_ACCESS_ALIGNED
        switch (hsz)
        {
#if __LP64__
        case 7: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 6: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 5: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 4: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
#endif
        case 3: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 2: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 1: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte; sz -= hsz;
        case 0:
            break;
        default:
            /* should never happen */
            break;
        }
#endif /* PMR_RAW_ACCESS_ALIGNED */

        /* words */

        T_WORD tword;

        while (sz >= sizeof(T_WORD))
        {
            tword = *p.word; *p.word++ = *q.word; *q.word++ = tword;
            sz -= sizeof(T_WORD);
        }

        /* tail */

        switch (sz)
        {
#if __LP64__
        case 7: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 6: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 5: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 4: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
#endif
        case 3: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 2: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 1: tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
        case 0:
            break;
        default:
            /* should never happen */
            break;
        }
    }
#if PMR_RAW_ACCESS_ALIGNED
    else
    {
        /* regions unaligned */

        uint8_t tbyte;

        while (sz != 0)
        {
            tbyte = *p.byte; *p.byte++ = *q.byte; *q.byte++ = tbyte;
            sz--;
        }
    }
#endif /* PMR_RAW_ACCESS_ALIGNED */

#else

    uint8_t * p = a;
    uint8_t * q = b;
    uint8_t t[128];

    while (sz >= sizeof(t))
    {
        PMR_MEMCPY(t, p, sizeof(t));
        PMR_MEMCPY(p, q, sizeof(t));
        PMR_MEMCPY(q, t, sizeof(t));

        p += sizeof(t);
        q += sizeof(t);
        sz -= sizeof(t);
    }

    if (sz > 0)
    {
        PMR_MEMCPY(t, p, sz);
        PMR_MEMCPY(p, q, sz);
        PMR_MEMCPY(q, t, sz);
    }

#endif
}

static inline void _region_copy(void * src, void * dst, size_t sz)
{
#if PMR_RAW_ACCESS

    ptr_t p = { src };
    ptr_t q = { dst };

    /* copy forward */

#if PMR_RAW_ACCESS_ALIGNED
    size_t hsz = p.uint & T_MASK;
    if (hsz == (q.uint & T_MASK))
#endif
    {
        /* regions aligned */

        /* head */

#if PMR_RAW_ACCESS_ALIGNED
        switch (hsz)
        {
#if __LP64__
        case 7: *q.byte++ = *p.byte++;
        case 6: *q.byte++ = *p.byte++;
        case 5: *q.byte++ = *p.byte++;
        case 4: *q.byte++ = *p.byte++;
#endif
        case 3: *q.byte++ = *p.byte++;
        case 2: *q.byte++ = *p.byte++;
        case 1: *q.byte++ = *p.byte++; sz -= hsz;
        case 0:
            break;
        default:
            /* should never happen */
            break;
        }
#endif /* PMR_RAW_ACCESS_ALIGNED */

        /* words */

        while (sz >= sizeof(T_WORD))
        {
            *q.word++ = *p.word++;
            sz -= sizeof(T_WORD);
        }

        /* tail */

        switch (sz)
        {
#if __LP64__
        case 7: *q.byte++ = *p.byte++;
        case 6: *q.byte++ = *p.byte++;
        case 5: *q.byte++ = *p.byte++;
        case 4: *q.byte++ = *p.byte++;
#endif
        case 3: *q.byte++ = *p.byte++;
        case 2: *q.byte++ = *p.byte++;
        case 1: *q.byte++ = *p.byte++;
        case 0:
            break;
        default:
            /* should never happen */
            break;
        }
    }
#if PMR_RAW_ACCESS_ALIGNED
    else
    {
        /* regions unaligned */

        while (sz != 0)
        {
            *q.byte++ = *p.byte++;
            sz--;
        }
    }
#endif /* PMR_RAW_ACCESS_ALIGNED */

#else

    PMR_MEMCPY(dst, src, sz);

#endif
}

static inline void _region_move_right(void * src, void * dst, size_t sz)
{
#if PMR_RAW_ACCESS

    ptr_t p = { src };
    ptr_t q = { dst };

    /* copy backward */

#if PMR_RAW_ACCESS_ALIGNED
    size_t hsz = p.uint & T_MASK;
    if (hsz == (q.uint & T_MASK))
#endif
    {
        /* regions aligned */

        /* tail */

        p.uint += sz;
        q.uint += sz;

#if PMR_RAW_ACCESS_ALIGNED
        size_t tsz = (sz - hsz) & T_MASK;
        switch (tsz)
        {
#if __LP64__
        case 7: *--q.byte = *--p.byte;
        case 6: *--q.byte = *--p.byte;
        case 5: *--q.byte = *--p.byte;
        case 4: *--q.byte = *--p.byte;
#endif
        case 3: *--q.byte = *--p.byte;
        case 2: *--q.byte = *--p.byte;
        case 1: *--q.byte = *--p.byte; sz -= tsz;
        case 0:
            break;
        default:
            /* should never happen */
            break;
        }
#endif /* PMR_RAW_ACCESS_ALIGNED */

        /* words */

        while (sz >= sizeof(T_WORD))
        {
            *--q.word = *--p.word;
            sz -= sizeof(T_WORD);
        }

        /* head */

        switch (sz)
        {
#if __LP64__
        case 7: *--q.byte = *--p.byte;
        case 6: *--q.byte = *--p.byte;
        case 5: *--q.byte = *--p.byte;
        case 4: *--q.byte = *--p.byte;
#endif
        case 3: *--q.byte = *--p.byte;
        case 2: *--q.byte = *--p.byte;
        case 1: *--q.byte = *--p.byte;
        case 0:
            break;
        default:
            /* should never happen */
            break;
        }
    }
#if PMR_RAW_ACCESS_ALIGNED
    else
    {
        /* regions unaligned */

        p.uint += sz;
        q.uint += sz;

        while (sz != 0)
        {
            *--q.byte = *--p.byte;
            sz--;
        }
    }
#endif /* PMR_RAW_ACCESS_ALIGNED */

#else

    PMR_MEMMOVE(dst, src, sz);

#endif
}

static inline void _region_move_left(void * src, void * dst, size_t sz)
{
#if PMR_RAW_ACCESS

    _region_copy(src, dst, sz);

#else

    PMR_MEMMOVE(dst, src, sz);

#endif
}

#if PMR_RAW_ACCESS
#if PMR_RAW_ACCESS_ALIGNED
#undef T_MASK
#endif
#undef T_WORD
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
/* allocate or adjust size of temporary storage if needed                                                                     */
/* -------------------------------------------------------------------------------------------------------------------------- */

static inline void * _aux_alloc(aux_t * aux, size_t sz)
{
    void * tmp = aux->temp;
    if (tmp == NULL || aux->sz < sz)
    {
        tmp = PMR_REALLOC(tmp, sz);
        if (tmp == NULL)
        {
            aux->rc = 1; /* FIXME: atomic */
        }
        else
        {
            aux->sz = sz;
            aux->temp = tmp;
        }
    }

    return tmp;
}

static inline void _aux_free(aux_t * aux)
{
    if (aux->temp != NULL)
    {
        PMR_FREE(aux->temp);
        aux->temp = NULL;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

#define IDIV_UP(N, M)               ({ __typeof__(N) __n = (N); __typeof__(M) __m = (M); (__n + (__m - 1)) / __m; })
#define ISIGN(V)                    ({ __typeof__(V) __v = (V); ((__v > 0) - (__v < 0)); })

#define ELT_PTR_FWD(ctx, base, inx) ELT_PTR_FWD_((base), (inx), ELT_SZ(ctx))
#define ELT_PTR_BCK(ctx, base, inx) ELT_PTR_BCK_((base), (inx), ELT_SZ(ctx))
#define ELT_PTR_NEXT(ctx, base)     (((void *)(base)) + ELT_SZ(ctx))
#define ELT_PTR_PREV(ctx, base)     (((void *)(base)) - ELT_SZ(ctx))
#define ELT_DIST(ctx, a, b)         ELT_DIST_((a), (b), ELT_SZ(ctx))

#define MAKE_STR0(x, y)             x ## y
#define MAKE_STR1(x, y)             MAKE_STR0(x, y)
#define MAKE_FNAME0(x, y)           _ ## x ## _ ## y
#define MAKE_FNAME1(x, y)           MAKE_FNAME0(x, y)
#define _M(name)                    MAKE_FNAME1(name, SORT_SUFFIX)
#define _F(name)                    MAKE_STR1(name, SORT_IS_R)
#define _(name)                     MAKE_FNAME1(name, MAKE_STR1(SORT_SUFFIX, SORT_IS_R))

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_USE_4_MEM

#define SORT_SUFFIX                 4

#define ELT_OF_SZ(n, sz)            ((n) << 2)
#define ELT_PTR_FWD_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) + (__inx << 2); })
#define ELT_PTR_BCK_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) - (__inx << 2); })
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) >> 2)

#define ELT_TYPE                    uint32_t
#include "pmergesort-mem-n.inl"

#include "pmergesort-mem.inl"

#undef ELT_TYPE
#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ

#undef SORT_SUFFIX

#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_USE_8_MEM

#define SORT_SUFFIX                 8

#define ELT_OF_SZ(n, sz)            ((n) << 3)
#define ELT_PTR_FWD_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) + (__inx << 3); })
#define ELT_PTR_BCK_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) - (__inx << 3); })
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) >> 3)

#if __LP64__
#define ELT_TYPE                    uint64_t
#include "pmergesort-mem-n.inl"
#else
#include "pmergesort-mem-sz.inl"
#endif

#include "pmergesort-mem.inl"

#undef ELT_TYPE
#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ

#undef SORT_SUFFIX

#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_USE_16_MEM

#define SORT_SUFFIX                 16

#define ELT_OF_SZ(n, sz)            ((n) << 4)
#define ELT_PTR_FWD_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) + (__inx << 4); })
#define ELT_PTR_BCK_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) - (__inx << 4); })
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) >> 4)

#include "pmergesort-mem-sz.inl"
#include "pmergesort-mem.inl"

#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ

#undef SORT_SUFFIX

#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#define SORT_SUFFIX                 sz

#define ELT_OF_SZ(n, sz)            ((sz) * (n))
#define ELT_PTR_FWD_(base, inx, sz) (((void *)(base)) + (sz) * (inx))
#define ELT_PTR_BCK_(base, inx, sz) (((void *)(base)) - (sz) * (inx))
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) / (sz))

#include "pmergesort-mem-sz.inl"
#include "pmergesort-mem.inl"

#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ

#undef SORT_SUFFIX

/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */

#define SORT_IS_R                   v
#define CALL_CMP(ctx, a, b)         ((cmpv_t)((ctx)->cmp))((a), (b))
#define CALL_SORT(ctx, a, n)        ((sort_t)((ctx)->wsort))((a), (n), (ctx)->sz, (cmpv_t)(ctx)->cmp)

#include "pmergesort.inl"

/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */

void symmergesort(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *))
{
    if (n < 2) /* have nothing to sort */
        return;

    context_t ctx = { base, n, sz, cmp, NULL, numCPU(), thPool(), 0, 0, cutOff(n), NULL, NULL, NULL };

    _F(symmergesort)(&ctx);
}

int pmergesort(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *))
{
    if (n < 2) /* have nothing to sort */
        return 0;

    context_t ctx = { base, n, sz, cmp, NULL, numCPU(), thPool(), 0, 0, cutOff(n), NULL, NULL, NULL };

    return _F(pmergesort)(&ctx);
}

int wrapmergesort(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *), int (*sort)(void *, size_t, size_t, int (*)(const void *, const void *)))
{
    if (n < 2) /* have nothing to sort */
        return 0;

    context_t ctx = { base, n, sz, cmp, NULL, numCPU(), thPool(), 0, 0, cutOff(n), NULL, NULL, sort };

    return _F(wrapmergesort)(&ctx);
}

#if _PMR_CORE_PROFILE
void insertionsort(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *))
{
    if (n < 2) /* have nothing to sort */
        return;

    context_t ctx = { base, n, sz, cmp, NULL, 0, NULL, 0, 0, 0, NULL, NULL, NULL };

    _F(insertionsort)(&ctx);
}
#endif

#if _PMR_CORE_PROFILE
void insertionsort_run(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *))
{
    if (n < 2) /* have nothing to sort */
        return;

    context_t ctx = { base, n, sz, cmp, NULL, 0, NULL, 0, 0, 0, NULL, NULL, NULL };

    _F(insertionsort_run)(&ctx);
}
#endif

#if _PMR_CORE_PROFILE
void insertionsort_mergerun(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *))
{
    if (n < 2) /* have nothing to sort */
        return;

    context_t ctx = { base, n, sz, cmp, NULL, 0, NULL, 0, 0, 0, NULL, NULL, NULL };

    _F(insertionsort_mergerun)(&ctx);
}
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#undef CALL_SORT
#undef CALL_CMP
#undef SORT_IS_R

/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */

#define SORT_IS_R                   r
#define CALL_CMP(ctx, a, b)         ((cmpr_t)((ctx)->cmp))((void *)(ctx)->thunk, (a), (b))
#define CALL_SORT(ctx, a, n)        ((sort_r_t)((ctx)->wsort))((a), (n), (ctx)->sz, (void *)(ctx)->thunk, (cmpr_t)(ctx)->cmp)

#include "pmergesort.inl"

/* -------------------------------------------------------------------------------------------------------------------------- */

void symmergesort_r(void * base, size_t n, size_t sz, void * thunk, int (*cmp)(void *, const void *, const void *))
{
    if (n < 2) /* have nothing to sort */
        return;

    context_t ctx = { base, n, sz, cmp, thunk, numCPU(), thPool(), 0, 0, cutOff(n), NULL, NULL, NULL };

    _F(symmergesort)(&ctx);
}

int pmergesort_r(void * base, size_t n, size_t sz, void * thunk, int (*cmp)(void *, const void *, const void *))
{
    if (n < 2) /* have nothing to sort */
        return 0;

    context_t ctx = { base, n, sz, cmp, thunk, numCPU(), thPool(), 0, 0, cutOff(n), NULL, NULL, NULL };

    return _F(pmergesort)(&ctx);
}

int wrapmergesort_r(void * base, size_t n, size_t sz, void * thunk, int (*cmp)(void *, const void *, const void *), int (*sort_r)(void *, size_t, size_t, void *, int (*)(void *, const void *, const void *)))
{
    if (n < 2) /* have nothing to sort */
        return 0;

    context_t ctx = { base, n, sz, cmp, thunk, numCPU(), thPool(), 0, 0, cutOff(n), NULL, NULL, sort_r };

    return _F(wrapmergesort)(&ctx);
}

/* -------------------------------------------------------------------------------------------------------------------------- */

#undef CALL_SORT
#undef CALL_CMP
#undef SORT_IS_R

/* -------------------------------------------------------------------------------------------------------------------------- */
