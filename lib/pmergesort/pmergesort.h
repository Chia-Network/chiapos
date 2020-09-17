/* -------------------------------------------------------------------------------------------------------------------------- */
/*  pmergesort.h                                                                                                              */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  Created by Cyril Murzin                                                                                                   */
/*  Copyright (c) 2015-2017 Ravel Developers Group. All rights reserved.                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */

#ifndef _PMERGESORT_H
#define _PMERGESORT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

    /* ---------------------------------------------------------------------------------------------------------------------- */
    /* inplace mergesort based on symmerge algorithm (parallel if configured)                                                 */
    /* ---------------------------------------------------------------------------------------------------------------------- */
    void symmergesort(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *));
    void symmergesort_r(void * base, size_t n, size_t sz, void * thunk, int (*cmp)(void *, const void *, const void *));
    /* ---------------------------------------------------------------------------------------------------------------------- */

    /* ---------------------------------------------------------------------------------------------------------------------- */
    /* out-of-place mergesort, naïve implementation (parallel if configured)                                                  */
    /* ---------------------------------------------------------------------------------------------------------------------- */
    int pmergesort(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *));
    int pmergesort_r(void * base, size_t n, size_t sz, void * thunk, int (*cmp)(void *, const void *, const void *));
    /* ---------------------------------------------------------------------------------------------------------------------- */

    /* ---------------------------------------------------------------------------------------------------------------------- */
    /* parallel wrapper for sort (parallel if configured, else meaningless)                                                   */
    /* ---------------------------------------------------------------------------------------------------------------------- */
    int wrapmergesort(void * base, size_t n, size_t sz, int (*cmp)(const void *, const void *),
                        int (*sort)(void *, size_t, size_t, int (*)(const void *, const void *)));
    int wrapmergesort_r(void * base, size_t n, size_t sz, void * thunk, int (*cmp)(void *, const void *, const void *),
                            int (*sort_r)(void *, size_t, size_t, void *, int (*)(void *, const void *, const void *)));
    /* ---------------------------------------------------------------------------------------------------------------------- */

#ifdef __cplusplus
}
#endif

#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
