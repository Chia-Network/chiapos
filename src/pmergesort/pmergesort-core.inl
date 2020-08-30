#ifndef __unused
#define __unused
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  pmergesort-core.inl                                                                                                       */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  Created by Cyril Murzin                                                                                                   */
/*  Copyright (c) 2015-2017 Ravel Developers Group. All rights reserved.                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  locate insertion point for given key in sorted segment                                                                    */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void * _(ip)(void * key, void * lo, void * hi, int sense, context_t * ctx)
{
    for (size_t lim = ELT_DIST(ctx, hi, lo); lim != 0; )
    {
        size_t half = lim >> 1;

        void * mid = ELT_PTR_FWD(ctx, lo, half);

        int result = CALL_CMP(ctx, key, mid);
        if (result > sense)
        {
            lo = ELT_PTR_NEXT(ctx, mid);
            lim = (lim - 1) >> 1;
        }
        else
        {
            /* hi = mid, but who cares? */
            lim = half;
        }
    }

    return lo;
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  locate the 1st non-matched pair of symmetric comparsion of [lo, hi) with [_, sym) for SymMerge algorithm (see below)      */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void * _(ip_sym)(void * sym, void * lo, void * hi, context_t * ctx)
{
    for (size_t lim = ELT_DIST(ctx, hi, lo); lim != 0; )
    {
        size_t half = lim >> 1;

        void * mid = ELT_PTR_FWD(ctx, lo, half);
        void * sym_mid = ELT_PTR_BCK(ctx, sym, half);

        int result = CALL_CMP(ctx, mid, sym_mid);
        if (result <= 0)
        {
            lo = ELT_PTR_NEXT(ctx, mid);
            sym = ELT_PTR_PREV(ctx, sym_mid);
            lim = (lim - 1) >> 1;
        }
        else
        {
            /* hi = mid, but who cares? */
            lim = half;
        }
    }

    return lo;
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  inplace merge two sorted segments of the vary sizes and keep resulting segment sorted                                     */
/*  [lo, mid) || [mid, hi) => [lo, hi)                                                                                        */
/*                                                                                                                            */
/*  linear merge uses binary search to locate insertion point, so consider it as                                              */
/*  binary insertion sort of specifically prepared segment                                                                    */
/*                                                                                                                            */
/*  assume right segnment is short, do not perform extra bounds checking                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(inplace_merge_r2l)(void * lo, void * mi, void * hi, context_t * ctx)
{
    size_t sz = ELT_SZ(ctx);

    while (mi < hi)
    {
        /* one iteration case:
            [X X Zi X X] [Zq Y Y] => [X X Zi Zq X X] [Y Y] */

        /* total length */
        size_t len = ELT_DIST(ctx, hi, lo);

        /* look for insertion point */

        void * ins;

        /* left segment size */
        size_t llen = ELT_DIST(ctx, mi, lo);

        if (llen < _PMR_MIN_SUBMERGELEN2)
        {
            /* linear search */
            ins = mi;
            while (lo < ins)
            {
                void * pins = ELT_PTR_PREV(ctx, ins);
                if (CALL_CMP(ctx, pins, mi) > 0)
                    ins = pins;
                else
                    break;
            }
        }
        else
        {
            /* binary search */
            ins = _(ip)(mi, lo, mi, -1, ctx);
        }

        if (ins == mi)
            break;

        /* look for source bounds */

        /* right segment size */
        size_t rlen = len - llen;

        void * nmi;
        if (rlen < _PMR_MIN_SUBMERGELEN2)
        {
            /* linear search */
            nmi = ELT_PTR_NEXT(ctx, mi);
            while (nmi < hi)
            {
                if (CALL_CMP(ctx, ins, nmi) > 0)
                    nmi = ELT_PTR_NEXT(ctx, nmi);
                else
                    break;
            }
        }
        else
        {
            /* binary search */
            nmi = _(ip)(ins, mi, hi, 0, ctx);
        }

        /* place subsegment at place (rotate is already _PMR_TMP_ROT aware and detects direction) */

        _M(rotate)(ins, mi, nmi, sz);

        /* advance */

        lo = ELT_PTR_FWD(ctx, ins, ELT_DIST(ctx, nmi, mi));
        mi = nmi;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  inplace merge two sorted segments of the vary sizes and keep resulting segment sorted                                     */
/*  [lo, mid) || [mid, hi) => [lo, hi)                                                                                        */
/*                                                                                                                            */
/*  linear merge uses binary search to locate insertion point, so consider it as                                              */
/*  binary insertion sort of specifically prepared segment                                                                    */
/*                                                                                                                            */
/*  assume left segnment is short, do not perform extra bounds checking                                                       */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(inplace_merge_l2r)(void * lo, void * mi, void * hi, context_t * ctx)
{
    size_t sz = ELT_SZ(ctx);

    while (lo < mi)
    {
        /* one iteration case:
            [X X Zq] [Y Y Zi Y Y] => [X X] [Y Y Zq Zi Y Y] */

        /* total length */
        size_t len = ELT_DIST(ctx, hi, lo);

        void * pmi = ELT_PTR_PREV(ctx, mi);

        /* look for insertion point */

        void * ins;

        /* right segment size */
        size_t rlen = ELT_DIST(ctx, hi, mi);

        if (rlen < _PMR_MIN_SUBMERGELEN2)
        {
            /* linear search */
            ins = mi;
            while (ins < hi)
            {
                if (CALL_CMP(ctx, ins, pmi) < 0)
                    ins = ELT_PTR_NEXT(ctx, ins);
                else
                    break;
            }
        }
        else
        {
            /* binary search */
            ins = _(ip)(pmi, mi, hi, 0, ctx);
        }

        if (ins == mi)
            break;

        /* look for source bounds */

        void * pins = ELT_PTR_PREV(ctx, ins);

        /* left segment size */
        size_t llen = len - rlen;

        if (llen < _PMR_MIN_SUBMERGELEN2)
        {
            /* linear search */
            while (lo < pmi)
            {
                void * ppmi = ELT_PTR_PREV(ctx, pmi);
                if (CALL_CMP(ctx, pins, ppmi) < 0)
                    pmi = ppmi;
                else
                    break;
            }
        }
        else
        {
            /* binary search */
            pmi = _(ip)(pins, lo, pmi, -1, ctx);
        }

        /* place subsegment at place (rotate is already _PMR_TMP_ROT aware and detects direction) */

        _M(rotate)(pmi, mi, ins, sz);

        /* advance */

        hi = ELT_PTR_BCK(ctx, ins, ELT_DIST(ctx, mi, pmi));
        mi = pmi;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  inplace merge two sorted segments of the vary sizes and keep resulting segment sorted                                     */
/*  [lo, mid) || [mid, hi) => [lo, hi)                                                                                        */
/*                                                                                                                            */
/*  perform bounds checking and detect short segment to invoke appropriate inplace merge                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(inplace_merge)(void * lo, void * mi, void * hi, context_t * ctx, __unused aux_t * aux)
{
    if (lo < mi && mi < hi)
    {
        if (mi - lo > hi - mi)
            _(inplace_merge_r2l)(lo, mi, hi, ctx);
        else
            _(inplace_merge_l2r)(lo, mi, hi, ctx);
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */

#if (PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS) && _PMR_PARALLEL_MAY_SPAWN
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  unified core of parallel symmerge                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------------- */
static
#if PMR_PARALLEL_USE_PTHREADS
inline
#endif
void _(merge_spawn_pass)(void * arg)
{
    pmergesort_pass_context_t * pass_ctx = arg;
    aux_t * aux = &pass_ctx->auxes[0];

    if (aux->rc == 0)
    {
#if PMR_PARALLEL_USE_GCD && !_PMR_GCD_OVERCOMMIT
        dispatch_semaphore_wait(pass_ctx->ctx->thpool->mutex, DISPATCH_TIME_FOREVER); /* semaphore to prevent the threads overcommit flood */
#endif

        aux_t laux;
        laux.rc = 0;
        laux.parent = aux;
        laux.sz = 0;
        laux.temp = NULL;

        pass_ctx->effector(pass_ctx->lo, pass_ctx->mi, pass_ctx->hi, pass_ctx->ctx, &laux);
        if (laux.rc != 0)
            aux->rc = laux.rc; /* FIXME: atomic */

        _aux_free(&laux);

#if PMR_PARALLEL_USE_GCD && !_PMR_GCD_OVERCOMMIT
        dispatch_semaphore_signal(pass_ctx->ctx->thpool->mutex); /* report processed */
#endif
    }

    PMR_FREE(arg); /* clean self */
}

#if PMR_PARALLEL_USE_PTHREADS
static void * _(merge_spawn_pass_ex)(void * arg)
{
    _(merge_spawn_pass)(arg);

    return NULL;
}
#endif
#endif /* (PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS) && _PMR_PARALLEL_MAY_SPAWN */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  inplace merge two sorted segments of the vary sizes and keep resulting segment sorted                                     */
/*  [lo, mi) || [mi, hi) => [lo, hi)                                                                                          */
/*                                                                                                                            */
/*  symmerge merges two sorted subsequences [lo, mi) and [mi, hi) using the                                                   */
/*  SymMerge algorithm from Pok-Son Kim and Arne Kutzner, "Stable Minimum                                                     */
/*  Storage Merging by Symmetric Comparisons", in Susanne Albers and Tomasz                                                   */
/*  Radzik, editors, Algorithms - ESA 2004, volume 3221 of Lecture Notes in                                                   */
/*  Computer Science, pages 714-723. Springer, 2004.                                                                          */
/* -------------------------------------------------------------------------------------------------------------------------- */
static void _(inplace_symmerge)(void * lo, void * mi, void * hi, context_t * ctx, aux_t * aux)
{
    size_t sz = ELT_SZ(ctx);

    while (lo < mi && mi < hi)
    {
        /* left segment size */
        size_t llen = ELT_DIST(ctx, mi, lo);

        /* fallback to linear merge for short segment at left */
        if (llen < _PMR_MIN_SUBMERGELEN1)
        {
            _(inplace_merge_l2r)(lo, mi, hi, ctx);
            break; /* we're done */
        }

        /* total length */
        size_t len = ELT_DIST(ctx, hi, lo);

        /* right segment size */
        size_t rlen = len - llen;

        /* fallback to linear merge for short segment at right */
        if (rlen < _PMR_MIN_SUBMERGELEN1)
        {
            _(inplace_merge_r2l)(lo, mi, hi, ctx);
            break; /* we're done */
        }

        /* else perform modified SymMerge algorithm */

        void * mid = ELT_PTR_FWD(ctx, lo, len >> 1);
        void * bound = ELT_PTR_FWD(ctx, mid, llen);

        void * start;

        if (mid < mi) /* llen > rlen */
        {
            void * lbound = ELT_PTR_BCK(ctx, bound, len);
            start = _(ip_sym)(ELT_PTR_PREV(ctx, hi), lbound, ELT_PTR_FWD(ctx, lbound, rlen), ctx);
        }
        else /* llen <= rlen */
        {
            start = _(ip_sym)(ELT_PTR_PREV(ctx, bound), lo, mi, ctx);
        }

/*      void * end = ELT_PTR_BCK(ctx, bound, ELT_DIST(ctx, start, lo)); */
        void * end = ELT_PTR_FWD(ctx, lo, ELT_DIST(ctx, bound, start));

        /* rotate side-changing elements */

        _M(rotate)(start, mi, end, sz);

        /* merge the 1st subsegments [lo, start) & [start, mid) recurrently
            (obviously it's the same size or shorter by 1 than the 2nd one) */

        if (lo < start && start < mid)
        {
#if _PMR_PARALLEL_MAY_SPAWN
#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS
            if (ctx->thpool != NULL && len > ctx->cut_off)
            {
                pmergesort_pass_context_t * pass_ctx = PMR_MALLOC(sizeof(pmergesort_pass_context_t));
                pass_ctx->ctx = ctx;
                pass_ctx->bsz = 0;
                pass_ctx->dbl_bsz = 0;
                pass_ctx->chunksz = 0;
                pass_ctx->numchunks = 0;
                pass_ctx->lo = lo;
                pass_ctx->mi = start;
                pass_ctx->hi = mid;
                pass_ctx->effector = ctx->merge_effector;
                pass_ctx->auxes = aux->parent;

#if PMR_PARALLEL_USE_PTHREADS
                thr_pool_queue(ctx->thpool, _(merge_spawn_pass_ex), pass_ctx);
#elif PMR_PARALLEL_USE_GCD
                dispatch_group_async_f(ctx->thpool->group, ctx->thpool->queue, pass_ctx, _(merge_spawn_pass));
#endif
            }
            else
                _(inplace_symmerge)(lo, start, mid, ctx, aux);
#elif PMR_PARALLEL_USE_OMP
            #pragma omp task if (len > ctx->cut_off) default(none) firstprivate(lo, start, mid, ctx)
            _(inplace_symmerge)(lo, start, mid, ctx, NULL);
#endif /* PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS */
#else
            _(inplace_symmerge)(lo, start, mid, ctx, NULL);
#endif /* _PMR_PARALLEL_MAY_SPAWN */
        }

        /* merge the 2nd subsegments [mid, end) & [end, hi) here instead of
            recurrent call of _(inplace_symmerge)(mid, end, hi, ctx, aux) */

        lo = mid;
        mi = end;
    /*  hi = hi; */
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  merge two sorted segments of the vary sizes and keep resulting segment sorted                                             */
/*  [lo, mi) || [mi, hi) => [lo, hi)                                                                                          */
/*                                                                                                                            */
/*  symmerge merges two sorted subsequences [lo, mi) and [mi, hi) using the                                                   */
/*  SymMerge algorithm from Pok-Son Kim and Arne Kutzner, "Stable Minimum                                                     */
/*  Storage Merging by Symmetric Comparisons", in Susanne Albers and Tomasz                                                   */
/*  Radzik, editors, Algorithms - ESA 2004, volume 3221 of Lecture Notes in                                                   */
/*  Computer Science, pages 714-723. Springer, 2004.                                                                          */
/*                                                                                                                            */
/*  may [re]allocate temporary storage                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------------- */
static __attribute__((unused)) void _(aux_symmerge)(void * lo, void * mi, void * hi, context_t * ctx, aux_t * aux)
{
#if PMR_PARALLEL_USE_OMP
    aux_t * paux = aux->parent;
#endif

    size_t sz = ELT_SZ(ctx);

    while (lo < mi && mi < hi)
    {
        /* left segment size */
        size_t llen = ELT_DIST(ctx, mi, lo);

        /* fallback to linear merge for short segment at left */
        if (llen < _PMR_MIN_SUBMERGELEN1)
        {
            _(inplace_merge_l2r)(lo, mi, hi, ctx);
            break; /* we're done */
        }

        /* total length */
        size_t len = ELT_DIST(ctx, hi, lo);

        /* right segment size */
        size_t rlen = len - llen;

        /* fallback to linear merge for short segment at right */
        if (rlen < _PMR_MIN_SUBMERGELEN1)
        {
            _(inplace_merge_r2l)(lo, mi, hi, ctx);
            break; /* we're done */
        }

        /* else perform modified SymMerge algorithm */

        void * mid = ELT_PTR_FWD(ctx, lo, len >> 1);
        void * bound = ELT_PTR_FWD(ctx, mid, llen);

        void * start;

        if (mid < mi) /* llen > rlen */
        {
            void * lbound = ELT_PTR_BCK(ctx, bound, len);
            start = _(ip_sym)(ELT_PTR_PREV(ctx, hi), lbound, ELT_PTR_FWD(ctx, lbound, rlen), ctx);
        }
        else /* llen <= rlen */
        {
            start = _(ip_sym)(ELT_PTR_PREV(ctx, bound), lo, mi, ctx);
        }

/*      void * end = ELT_PTR_BCK(ctx, bound, ELT_DIST(ctx, start, lo)); */
        void * end = ELT_PTR_FWD(ctx, lo, ELT_DIST(ctx, bound, start));

        /* rotate side-changing elements */

        _M(rotate_aux)(start, mi, end, sz, aux);
        if (aux->rc != 0)
            break; /* bail out */

        /* merge the 1st subsegments [lo, start) & [start, mid) recurrently
            (obviously it's the same size or shorter by 1 than the 2nd one) */

        if (lo < start && start < mid)
        {
#if _PMR_PARALLEL_MAY_SPAWN
#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS
            if (ctx->thpool != NULL && len > ctx->cut_off)
            {
                pmergesort_pass_context_t * pass_ctx = PMR_MALLOC(sizeof(pmergesort_pass_context_t));
                pass_ctx->ctx = ctx;
                pass_ctx->bsz = 0;
                pass_ctx->dbl_bsz = 0;
                pass_ctx->chunksz = 0;
                pass_ctx->numchunks = 0;
                pass_ctx->lo = lo;
                pass_ctx->mi = start;
                pass_ctx->hi = mid;
                pass_ctx->effector = ctx->merge_effector;
                pass_ctx->auxes = aux->parent;

#if PMR_PARALLEL_USE_PTHREADS
                thr_pool_queue(ctx->thpool, _(merge_spawn_pass_ex), pass_ctx);
#elif PMR_PARALLEL_USE_GCD
                dispatch_group_async_f(ctx->thpool->group, ctx->thpool->queue, pass_ctx, _(merge_spawn_pass));
#endif
            }
            else
            {
                _(aux_symmerge)(lo, start, mid, ctx, aux);

                if (aux->rc != 0)
                    break; /* bail out */
            }
#elif PMR_PARALLEL_USE_OMP
            #pragma omp task if (len > ctx->cut_off) default(none) firstprivate(lo, start, mid, ctx, paux)
            if (paux->rc == 0)
            {
                aux_t laux;
                laux.rc = 0;
                laux.parent = paux;
                laux.sz = 0;
                laux.temp = NULL;

                _(aux_symmerge)(lo, start, mid, ctx, &laux);
                if (laux.rc != 0)
                    paux->rc = laux.rc; /* FIXME: atomic */

                _aux_free(&laux);
            }
#endif /* PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS */
#else
            _(aux_symmerge)(lo, start, mid, ctx, aux);

            if (aux->rc != 0)
                break; /* bail out */
#endif /* _PMR_PARALLEL_MAY_SPAWN */
        }

        /* merge the 2nd subsegments [mid, end) & [end, hi) here instead of
            recurrent call of _(aux_symmerge)(mid, end, hi, ctx, aux) */

        lo = mid;
        mi = end;
    /*  hi = hi; */
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  merge two sorted segments of the vary sizes and keep resulting segment sorted                                             */
/*  [lo, mid) || [mid, hi) => [lo, hi)                                                                                        */
/*                                                                                                                            */
/*  naïve merge implementation, uses binary search to locate merge bounds and concatenation case                              */
/*  may [re]allocate temporary storage                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  merge two sorted segments of the vary sizes and keep resulting segment sorted                                             */
/*  assume right segnment is short, do not perform extra bounds checking                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(aux_merge_r)(void * lo, void * mi, void * hi, context_t * ctx, aux_t * aux)
{
    size_t rsz = ELT_DIST(ctx, hi, mi);

    /* fallback to linear merge for short segment at right */
    if (rsz < _PMR_MIN_SUBMERGELEN1)
    {
        _(inplace_merge_r2l)(lo, mi, hi, ctx);
        return; /* we're done */
    }

    size_t sz = ELT_SZ(ctx);

    /* allocate or adjust size of temporary storage if needed, caller will free it */
    void * tmp = _aux_alloc(aux, ELT_OF_SZ(rsz, sz));
    if (tmp == NULL)
        return; /* bail out due to the not enough memory error */

    /* copy right segment's content to temporary storage */
    _M(copy)(mi, tmp, rsz, sz);

    /* merge from left to right */

    void * dst = hi;

    void * lsrclo = lo;
    void * lsrchi = ELT_PTR_PREV(ctx, mi);

    void * rsrclo = tmp;
    void * rsrchi = ELT_PTR_FWD(ctx, tmp, rsz - 1);

    /* merge */
    while (lsrchi >= lsrclo && rsrchi >= rsrclo)
    {
        dst = ELT_PTR_PREV(ctx, dst);

        int rc = CALL_CMP(ctx, lsrchi, rsrchi);
        if (rc > 0)
        {
            _M(copy)(lsrchi, dst, 1, sz);
            lsrchi = ELT_PTR_PREV(ctx, lsrchi);
        }
        else
        {
            _M(copy)(rsrchi, dst, 1, sz);
            rsrchi = ELT_PTR_PREV(ctx, rsrchi);
        }
    }

    /* copy right tail from temporary storage */
    if (rsrchi >= rsrclo)
    {
        size_t tailsz = ELT_DIST(ctx, rsrchi, rsrclo) + 1;

        _M(copy)(rsrclo, ELT_PTR_BCK(ctx, dst, tailsz), tailsz, sz);
    }

    /* no need to copy from left to itself */
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  merge two sorted segments of the vary sizes and keep resulting segment sorted                                             */
/*  assume left segnment is short, do not perform extra bounds checking                                                       */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(aux_merge_l)(void * lo, void * mi, void * hi, context_t * ctx, aux_t * aux)
{
    size_t lsz = ELT_DIST(ctx, mi, lo);

    /* fallback to linear merge for short segment at left */
    if (lsz < _PMR_MIN_SUBMERGELEN1)
    {
        _(inplace_merge_l2r)(lo, mi, hi, ctx);
        return; /* we're done */
    }

    size_t sz = ELT_SZ(ctx);

    /* allocate or adjust size of temporary storage if needed, caller will free it */
    void * tmp = _aux_alloc(aux, ELT_OF_SZ(lsz, sz));
    if (tmp == NULL)
        return; /* bail out due to the not enough memory error */

    /* copy left segment's content to temporary storage */
    _M(copy)(lo, tmp, lsz, sz);

    /* merge from right to left */

    void * dst = lo;

    void * lsrclo = tmp;
    void * lsrchi = ELT_PTR_FWD(ctx, tmp, lsz);

    void * rsrclo = mi;
    void * rsrchi = hi;

    /* merge */
    while (lsrclo < lsrchi && rsrclo < rsrchi)
    {
        int rc = CALL_CMP(ctx, lsrclo, rsrclo);
        if (rc <= 0)
        {
            _M(copy)(lsrclo, dst, 1, sz);
            lsrclo = ELT_PTR_NEXT(ctx, lsrclo);
        }
        else
        {
            _M(copy)(rsrclo, dst, 1, sz);
            rsrclo = ELT_PTR_NEXT(ctx, rsrclo);
        }

        dst = ELT_PTR_NEXT(ctx, dst);
    }

    /* copy left tail from temporary storage */
    if (lsrclo < lsrchi)
    {
        _M(copy)(lsrclo, dst, ELT_DIST(ctx, lsrchi, lsrclo), sz);
    }

    /* no need to copy from right to itself */
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  naïve merge two sorted segments of the vary sizes and keep resulting segment sorted                                       */
/*  [lo, mid) || [mid, hi) => [lo, hi)                                                                                        */
/*                                                                                                                            */
/*  perform bounds checking and detect short segment to invoke appropriate merge                                              */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(aux_merge)(void * lo, void * mi, void * hi, context_t * ctx, aux_t * aux)
{
    if (lo < mi && mi < hi)
    {
        /* find minimal bounds to operate */

        void * inslo = _(ip)(mi, lo, mi, -1, ctx);
        if (inslo == mi)
            return; /* we're done */

        void * inshi = _(ip)(ELT_PTR_PREV(ctx, mi), mi, hi, 0, ctx);

#if 1
        /* should just swap segments */

        if (CALL_CMP(ctx, inslo, ELT_PTR_PREV(ctx, inshi)) > 0)
        {
            _M(rotate_aux)(inslo, mi, inshi, ELT_SZ(ctx), aux);
            return; /* we're done */
        }
#endif

        /* merge shortest segment */

        if (mi - inslo > inshi - mi)
            _(aux_merge_r)(inslo, mi, inshi, ctx, aux);
        else
            _(aux_merge_l)(inslo, mi, inshi, ctx, aux);
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  find and normalize segment wich contains elements in ascending order                                                      */
/*  [lo, hi) => [lo, end]                                                                                                     */
/*                                                                                                                            */
/*  returns the last element of normalized segment, or end >= hi when failed                                                  */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void * _(next_run)(void * lo, void * hi, context_t * ctx)
{
    if (lo < hi)
    {
        /* initial elements relations, non-zero means established order */

        void * cur = ELT_PTR_NEXT(ctx, lo);
        if (cur >= hi)
            return lo;

        /* expected order of segment */
        int r = ISIGN(CALL_CMP(ctx, lo, cur));

        while (cur < hi)
        {
            void * ncur = ELT_PTR_NEXT(ctx, cur);
            if (ncur >= hi)
                break;

            /* sibling relation */
            int rc = ISIGN(CALL_CMP(ctx, cur, ncur));

            if (r == rc)
            {
                /* same as expected order */

                cur = ncur;
            }
            else
            {
                /* order changed */

                if (rc == 0)
                {
                    /* next element same as previous element */

                    if (r > 0) /* was descending, no continue since it's stable sort */
                        break;

                    cur = ncur;
                }
                else if (r == 0)
                {
                    /* run initiated with equal elements sequence */

                    if (rc > 0) /* assume was ascending for stable */
                        break;

                    cur = ncur;
                    r = rc; /* override expected order */
                }
                else
                    break; /* end of segment of expected order */
            }
        }

        if (r > 0)
        {
            /* reverse descending segment */
            _M(reverse)(lo, cur, ELT_SZ(ctx));
        }

        return cur;
    }
    else
        return lo;
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  binary insertion sort of segment                                                                                          */
/*  [lo, hi) => [lo, hi)                                                                                                      */
/*                                                                                                                            */
/*  mi is the hint point which marks end of first sorted subsegment (basically mi == lo)                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(binsort)(void * lo, void * mi, void * hi, context_t * ctx, __unused aux_t * aux)
{
    size_t sz = ELT_SZ(ctx);

    mi = ELT_PTR_NEXT(ctx, mi); /* advance mi to mark left subsegment (in canonical case is the second element of array) */
    size_t llen = ELT_DIST(ctx, mi, lo); /* length of left subsegment */

    while (mi < hi)
    {
        /* look for insertion point */

        void * ins;

        if (llen < _PMR_MIN_SUBMERGELEN2)
        {
            /* linear search */
            ins = mi;
            while (lo < ins)
            {
                void * pins = ELT_PTR_PREV(ctx, ins);
                if (CALL_CMP(ctx, pins, mi) > 0)
                    ins = pins;
                else
                    break;
            }
        }
        else
        {
            /* binary search */
            ins = _(ip)(mi, lo, mi, -1, ctx);
        }

        if (ins < mi)
        {
            /* insert mi element at place */

            uint8_t t[ELT_OF_SZ(1, sz)];

            _M(copy)(mi, t, 1, sz);
            _M(move_right)(ins, ELT_DIST(ctx, mi, ins), 1, sz);
            _M(copy)(t, ins, 1, sz);
        }

        /* advance */

        mi = ELT_PTR_NEXT(ctx, mi);
        llen++;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  binary insertion sort of segment with finding the longest presorted run at start                                          */
/*  [lo, hi) => [lo, hi)                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(binsort_run)(void * lo, __unused void * mi, void * hi, context_t * ctx, __unused aux_t * aux)
{
    void * end = _(next_run)(lo, hi, ctx);
    if (end < hi)
        _(binsort)(lo, end, hi, ctx, NULL);
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  binary insertion sort of segment with finding the longest presorted runs to submerge                                      */
/*  [lo, hi) => [lo, hi)                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _(binsort_mergerun)(void * lo, __unused void * mi, void * hi, context_t * ctx, __unused aux_t * aux)
{
    void * end = ELT_PTR_NEXT(ctx, _(next_run)(lo, hi, ctx));
    while (end < hi)
    {
        void * end0 = ELT_PTR_NEXT(ctx, _(next_run)(end, hi, ctx));
        if (end0 > hi)
            end0 = hi;

        /* merge two segments */
        _(inplace_merge)(lo, end, end0, ctx, NULL);

        end = end0;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */

#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  unified core of parallel mergesort                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------------- */
static
#if PMR_PARALLEL_USE_PTHREADS
inline
#endif
void _(sort_chunk_pass)(void * arg, size_t chunk)
{
    pmergesort_pass_context_t * pass_ctx = arg;
    aux_t * aux = &pass_ctx->auxes[chunk];

    int last = (chunk < pass_ctx->numchunks - 1) ? 0 : 1;

    void * a = ELT_PTR_FWD(pass_ctx->ctx, pass_ctx->lo, pass_ctx->chunksz * chunk);
    void * b = ELT_PTR_FWD(pass_ctx->ctx, a, pass_ctx->bsz);
    void * c = last == 0 ? ELT_PTR_FWD(pass_ctx->ctx, a, pass_ctx->chunksz) : pass_ctx->hi;

    while (b <= c)
    {
        pass_ctx->effector(a, a, b, pass_ctx->ctx, aux);
        if (aux->rc != 0)
            break;

        a = b;
        b = ELT_PTR_FWD(pass_ctx->ctx, b, pass_ctx->bsz);
    }

    if (last != 0 && aux->rc == 0)
        pass_ctx->effector(a, a, c, pass_ctx->ctx, aux);
}

static
#if PMR_PARALLEL_USE_PTHREADS
inline
#endif
void _(merge_chunks_pass)(void * arg, size_t chunk)
{
    pmergesort_pass_context_t * pass_ctx = arg;

#if PMR_PARALLEL_USE_GCD && _PMR_PARALLEL_MAY_SPAWN && !_PMR_GCD_OVERCOMMIT
    dispatch_semaphore_wait(pass_ctx->ctx->thpool->mutex, DISPATCH_TIME_FOREVER); /* semaphore to prevent the threads overcommit flood */
#endif

    aux_t * aux = &pass_ctx->auxes[chunk];

    int last = (chunk < pass_ctx->numchunks - 1) ? 0 : 1;

    void * a = ELT_PTR_FWD(pass_ctx->ctx, pass_ctx->lo, pass_ctx->chunksz * chunk);
    void * b = ELT_PTR_FWD(pass_ctx->ctx, a, pass_ctx->dbl_bsz);
    void * c = last == 0 ? ELT_PTR_FWD(pass_ctx->ctx, a, pass_ctx->chunksz) : pass_ctx->hi;

    while (b <= c)
    {
        pass_ctx->effector(a, ELT_PTR_FWD(pass_ctx->ctx, a, pass_ctx->bsz), b, pass_ctx->ctx, aux);
        if (aux->rc != 0)
            break;

        a = b;
        b = ELT_PTR_FWD(pass_ctx->ctx, b, pass_ctx->dbl_bsz);
    }

    if (last != 0 && aux->rc == 0)
        pass_ctx->effector(a, ELT_PTR_FWD(pass_ctx->ctx, a, pass_ctx->bsz), c, pass_ctx->ctx, aux);

#if PMR_PARALLEL_USE_GCD && _PMR_PARALLEL_MAY_SPAWN && !_PMR_GCD_OVERCOMMIT
    dispatch_semaphore_signal(pass_ctx->ctx->thpool->mutex); /* report processed */
#endif
}

#if PMR_PARALLEL_USE_PTHREADS
static void * _(sort_chunk_pass_ex)(void * arg)
{
    _(sort_chunk_pass)(arg, ((pmergesort_pass_context_t *)arg)->chunk);

    return NULL;
}

static void * _(merge_chunks_pass_ex)(void * arg)
{
    _(merge_chunks_pass)(arg, ((pmergesort_pass_context_t *)arg)->chunk);

    return NULL;
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  unified core of parallel mergesort based on pthreads pool                                                                 */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline int _(pmergesort_impl)(context_t * ctx)
{
    aux_t auxes[ctx->ncpu];
    for (int i = 0; i < ctx->ncpu; i++)
        auxes[i] = (aux_t){ .parent = &auxes[i] };

    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    size_t bsz = ctx->bsize;
    size_t npercpu = ctx->npercpu;

    /* pass 1 */
    {
        /* divide the array up into up to ncores, multiple-of-block-sized, chunks */
        size_t chunksz = IDIV_UP(npercpu, bsz) * bsz;
        size_t numchunks = IDIV_UP(ctx->n, chunksz);

        pmergesort_pass_context_t pass1_ctx_base;
        pass1_ctx_base.ctx = ctx;
        pass1_ctx_base.bsz = bsz;
        pass1_ctx_base.chunksz = chunksz;
        pass1_ctx_base.numchunks = numchunks;
        pass1_ctx_base.lo = lo;
        pass1_ctx_base.mi = NULL;
        pass1_ctx_base.hi = hi;
        pass1_ctx_base.effector = ctx->sort_effector;
        pass1_ctx_base.auxes = auxes;

        if (numchunks > 1)
        {
            pmergesort_pass_context_t pass1_ctx[numchunks];

            for (size_t chunk = 0; chunk < numchunks; chunk++)
            {
                pass1_ctx[chunk] = pass1_ctx_base;
                pass1_ctx[chunk].chunk = chunk;

                thr_pool_queue(ctx->thpool, _(sort_chunk_pass_ex), &pass1_ctx[chunk]);
            }

            thr_pool_wait(ctx->thpool);

            for (int i = 0; i < numchunks; i++)
            {
                if (auxes[i].rc != 0)
                    goto bail_out;
            }
        }
        else
        {
            pass1_ctx_base.chunk = 0;
            _(sort_chunk_pass_ex)(&pass1_ctx_base);

            goto bail_out;
        }
    }

    /* pass 2 */
    pmergesort_pass_context_t pass2_ctx_base;
    pass2_ctx_base.ctx = ctx;
    pass2_ctx_base.lo = lo;
    pass2_ctx_base.mi = NULL;
    pass2_ctx_base.hi = hi;
    pass2_ctx_base.effector = ctx->merge_effector;
    pass2_ctx_base.auxes = auxes;

    while (bsz < ctx->n)
    {
        /* divide the array up into up to ncores, multiple-of-double-block-sized, chunks */
        size_t dbl_bsz = bsz << 1;

        size_t chunksz = IDIV_UP(npercpu, dbl_bsz) * dbl_bsz;
        size_t numchunks = IDIV_UP(ctx->n, chunksz);

        pass2_ctx_base.bsz = bsz;
        pass2_ctx_base.dbl_bsz = dbl_bsz;
        pass2_ctx_base.chunksz = chunksz;
        pass2_ctx_base.numchunks = numchunks;

        if (numchunks > 1)
        {
            /* let's be less greedy for temporary memory */
            for (int i = (int)numchunks; i < ctx->ncpu; i++)
                _aux_free(&auxes[i]);

            pmergesort_pass_context_t pass2_ctx[numchunks];

            for (size_t chunk = 0; chunk < numchunks; chunk++)
            {
                pass2_ctx[chunk] = pass2_ctx_base;
                pass2_ctx[chunk].chunk = chunk;

                thr_pool_queue(ctx->thpool, _(merge_chunks_pass_ex), &pass2_ctx[chunk]);
            }

            thr_pool_wait(ctx->thpool);

            for (int i = 0; i < numchunks; i++)
            {
                if (auxes[i].rc != 0)
                    goto bail_out;
            }
        }
        else
        {
            pass2_ctx_base.chunk = 0;
            _(merge_chunks_pass_ex)(&pass2_ctx_base);

#if _PMR_PARALLEL_MAY_SPAWN
            thr_pool_wait(ctx->thpool);
#endif
        }

        bsz = dbl_bsz;
    }

bail_out:;

    int rc = 0;
    for (int i = 0; i < ctx->ncpu; i++)
    {
        _aux_free(&auxes[i]);

        if (rc == 0)
            rc = auxes[i].rc;
    }

    return rc;
}
#elif PMR_PARALLEL_USE_GCD
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  unified core of parallel mergesort based on GCD                                                                           */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline int _(pmergesort_impl)(context_t * ctx)
{
    dispatch_queue_t queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, _PMR_DISPATCH_QUEUE_FLAGS);

    thr_pool_t pool;
    pool.queue = queue;
#if _PMR_PARALLEL_MAY_SPAWN
    pool.group = NULL;
#if !_PMR_GCD_OVERCOMMIT
    pool.mutex = NULL;
#endif
#endif

    ctx->thpool = &pool;

    aux_t auxes[ctx->ncpu];
    for (int i = 0; i < ctx->ncpu; i++)
        auxes[i] = (aux_t){ .parent = &auxes[i] };

    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    size_t bsz = ctx->bsize;
    size_t npercpu = ctx->npercpu;

    /* pass 1 */
    {
        /* divide the array up into up to ncores, multiple-of-block-sized, chunks */
        size_t chunksz = IDIV_UP(npercpu, bsz) * bsz;
        size_t numchunks = IDIV_UP(ctx->n, chunksz);

        pmergesort_pass_context_t pass1_ctx;
        pass1_ctx.ctx = ctx;
        pass1_ctx.bsz = bsz;
        pass1_ctx.chunksz = chunksz;
        pass1_ctx.numchunks = numchunks;
        pass1_ctx.lo = lo;
        pass1_ctx.mi = NULL;
        pass1_ctx.hi = hi;
        pass1_ctx.effector = ctx->sort_effector;
        pass1_ctx.auxes = auxes;

        if (numchunks > 1)
        {
            dispatch_apply_f(numchunks, queue, &pass1_ctx, _(sort_chunk_pass));

            for (int i = 0; i < numchunks; i++)
            {
                if (auxes[i].rc != 0)
                    goto bail_out;
            }
        }
        else
        {
            _(sort_chunk_pass)(&pass1_ctx, 0);

            goto bail_out;
        }
    }

#if _PMR_PARALLEL_MAY_SPAWN
    pool.group = dispatch_group_create();
#if !_PMR_GCD_OVERCOMMIT
    pool.mutex = dispatch_semaphore_create(ctx->ncpu);
#endif
#endif

    /* pass 2 */
    {
        pmergesort_pass_context_t pass2_ctx;
        pass2_ctx.ctx = ctx;
        pass2_ctx.lo = lo;
        pass2_ctx.mi = NULL;
        pass2_ctx.hi = hi;
        pass2_ctx.effector = ctx->merge_effector;
        pass2_ctx.auxes = auxes;

        while (bsz < ctx->n)
        {
            /* divide the array up into up to ncores, multiple-of-double-block-sized, chunks */
            size_t dbl_bsz = bsz << 1;

            size_t chunksz = IDIV_UP(npercpu, dbl_bsz) * dbl_bsz;
            size_t numchunks = IDIV_UP(ctx->n, chunksz);

            pass2_ctx.bsz = bsz;
            pass2_ctx.dbl_bsz = dbl_bsz;
            pass2_ctx.chunksz = chunksz;
            pass2_ctx.numchunks = numchunks;

            if (numchunks > 1)
            {
                /* let's be less greedy for temporary memory */
                for (int i = (int)numchunks; i < ctx->ncpu; i++)
                    _aux_free(&auxes[i]);

                dispatch_apply_f(numchunks, queue, &pass2_ctx, _(merge_chunks_pass));

#if _PMR_PARALLEL_MAY_SPAWN
                dispatch_group_wait(pool.group, DISPATCH_TIME_FOREVER);
#endif

                for (int i = 0; i < numchunks; i++)
                {
                    if (auxes[i].rc != 0)
                        goto bail_out;
                }
            }
            else
            {
                _(merge_chunks_pass)(&pass2_ctx, 0);
#if _PMR_PARALLEL_MAY_SPAWN
                dispatch_group_wait(pool.group, DISPATCH_TIME_FOREVER);
#endif
            }

            bsz = dbl_bsz;
        }
    }

bail_out:;

#if _PMR_PARALLEL_MAY_SPAWN
    if (pool.group != NULL)
        dispatch_release(DISPATCH_OBJECT_T(pool.group));
#if !_PMR_GCD_OVERCOMMIT
    if (pool.mutex != NULL)
        dispatch_release(DISPATCH_OBJECT_T(pool.mutex));
#endif
#endif

    int rc = 0;
    for (int i = 0; i < ctx->ncpu; i++)
    {
        _aux_free(&auxes[i]);

        if (rc == 0)
            rc = auxes[i].rc;
    }

    return rc;
}
#endif
#elif PMR_PARALLEL_USE_OMP
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  unified core of parallel mergesort based on OpenMP                                                                        */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline int _(pmergesort_impl)(context_t * ctx)
{
    aux_t auxes[ctx->ncpu];
    for (int i = 0; i < ctx->ncpu; i++)
        auxes[i] = (aux_t){ .parent = &auxes[i] };

    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    size_t bsz = ctx->bsize;
    size_t npercpu = ctx->npercpu;

    /* pass 1 */
    {
        /* divide the array up into up to ncores, multiple-of-block-sized, chunks */
        size_t chunksz = IDIV_UP(npercpu, bsz) * bsz;
        size_t numchunks = IDIV_UP(ctx->n, chunksz);

        #pragma omp parallel num_threads(numchunks)
        #pragma omp for
        for (size_t chunk = 0; chunk < numchunks; chunk++)
        {
            aux_t * aux = &auxes[chunk];

            int last = (chunk < numchunks - 1) ? 0 : 1;

            void * a = ELT_PTR_FWD(ctx, lo, chunksz * chunk);
            void * b = ELT_PTR_FWD(ctx, a, bsz);
            void * c = last == 0 ? ELT_PTR_FWD(ctx, a, chunksz) : hi;

            while (b <= c)
            {
                ctx->sort_effector(a, a, b, ctx, aux);
                if (aux->rc != 0)
                    break;

                a = b;
                b = ELT_PTR_FWD(ctx, b, bsz);
            }

            if (last != 0 && aux->rc == 0)
                ctx->sort_effector(a, a, c, ctx, aux);
        }

        for (int i = 0; i < numchunks; i++)
        {
            if (auxes[i].rc != 0)
                goto bail_out;
        }
    }

    /* pass 2 */
    {
        /* num. threads override */
        __unused size_t ncpu = ctx->thpool != NULL ? ctx->thpool->ncpu : 0;

        while (bsz < ctx->n)
        {
            /* divide the array up into up to ncores, multiple-of-double-block-sized, chunks */
            size_t dbl_bsz = bsz << 1;

            size_t chunksz = IDIV_UP(npercpu, dbl_bsz) * dbl_bsz;
            size_t numchunks = IDIV_UP(ctx->n, chunksz);

            #pragma omp parallel num_threads(ncpu != 0 ? ncpu : numchunks)
            #pragma omp for
            for (size_t chunk = 0; chunk < numchunks; chunk++)
            {
                aux_t * aux = &auxes[chunk];

                int last = (chunk < numchunks - 1) ? 0 : 1;

                void * a = ELT_PTR_FWD(ctx, lo, chunksz * chunk);
                void * b = ELT_PTR_FWD(ctx, a, dbl_bsz);
                void * c = last == 0 ? ELT_PTR_FWD(ctx, a, chunksz) : hi;

                while (b <= c)
                {
                    ctx->merge_effector(a, ELT_PTR_FWD(ctx, a, bsz), b, ctx, aux);
                    if (aux->rc != 0)
                        break;

                    a = b;
                    b = ELT_PTR_FWD(ctx, b, dbl_bsz);
                }

                if (last != 0 && aux->rc == 0)
                    ctx->merge_effector(a, ELT_PTR_FWD(ctx, a, bsz), c, ctx, aux);
            }

            #pragma omp taskwait

            for (int i = 0; i < numchunks; i++)
            {
                if (auxes[i].rc != 0)
                    goto bail_out;
            }

            bsz = dbl_bsz;
        }
    }

bail_out:;

    int rc = 0;
    for (int i = 0; i < ctx->ncpu; i++)
    {
        _aux_free(&auxes[i]);

        if (rc == 0)
            rc = auxes[i].rc;
    }

    return rc;
}
#endif /* PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS */

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  in-place mergesort (symmerge based)                                                                                       */
/* -------------------------------------------------------------------------------------------------------------------------- */

static inline void _(symmergesort)(context_t * ctx)
{
    if (ctx->n < _PMR_BLOCKLEN_MTHRESHOLD0 * _PMR_BLOCKLEN_SYMMERGE)
    {
        void * lo = (void *)ctx->base;
        void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

        _(_PMR_PRESORT)(lo, lo, hi, ctx, NULL);

        return;
    }

#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS || PMR_PARALLEL_USE_OMP
    for (int ncpu = ctx->ncpu; ncpu > 1; ncpu--)
    {
        size_t npercpu = IDIV_UP(ctx->n, ncpu);
        if (npercpu >= _PMR_BLOCKLEN_MTHRESHOLD * _PMR_BLOCKLEN_SYMMERGE)
        {
            /* use parallel when have a long enough array could be distributed by cores */

            /* pre-set initial pass values */
            ctx->npercpu = npercpu;
            ctx->bsize = _PMR_BLOCKLEN_SYMMERGE;
            ctx->sort_effector = _(_PMR_PRESORT);
            ctx->merge_effector = _(inplace_symmerge);

#if PMR_PARALLEL_USE_OMP && _PMR_PARALLEL_MAY_SPAWN
            thr_pool_t pool;
            pool.ncpu = ctx->ncpu; /* utilize maximum number of cores when symmerge spawns threads */

            ctx->thpool = &pool;
#endif

            /* run parallel sort */
            (void)_(pmergesort_impl)(ctx);

            return;
        }
    }
#endif

#if (PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS) && _PMR_PARALLEL_MAY_SPAWN
    ctx->thpool = NULL; /* disable threads spawn */
#endif

    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    size_t bsz = _PMR_BLOCKLEN_SYMMERGE;

    void * a = lo;
    void * b = ELT_PTR_FWD(ctx, a, bsz);

    while (b <= hi)
    {
        _(_PMR_PRESORT)(a, a, b, ctx, NULL);

        a = b;
        b = ELT_PTR_FWD(ctx, b, bsz);
    }

    _(_PMR_PRESORT)(a, a, hi, ctx, NULL);

    while (bsz < ctx->n)
    {
        size_t bsz1 = bsz << 1;

        a = lo;
        b = ELT_PTR_FWD(ctx, a, bsz1);

        while (b <= hi)
        {
            _(inplace_symmerge)(a, ELT_PTR_FWD(ctx, a, bsz), b, ctx, NULL);

            a = b;
            b = ELT_PTR_FWD(ctx, b, bsz1);
        }

        _(inplace_symmerge)(a, ELT_PTR_FWD(ctx, a, bsz), hi, ctx, NULL);

        bsz = bsz1;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  naïve mergesort implementation                                                                                            */
/* -------------------------------------------------------------------------------------------------------------------------- */

static inline int _(pmergesort)(context_t * ctx)
{
    if (ctx->n < _PMR_BLOCKLEN_MTHRESHOLD0 * _PMR_BLOCKLEN_SYMMERGE)
    {
        void * lo = (void *)ctx->base;
        void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

        _(_PMR_PRESORT)(lo, lo, hi, ctx, NULL);

        return 0;
    }

#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS || PMR_PARALLEL_USE_OMP
    for (int ncpu = ctx->ncpu; ncpu > 1; ncpu--)
    {
        size_t npercpu = IDIV_UP(ctx->n, ncpu);
        if (npercpu >= _PMR_BLOCKLEN_MTHRESHOLD * _PMR_BLOCKLEN_MERGE)
        {
            /* use parallel when have a long enough array could be distributed by cores */

            /* pre-set initial pass values */
            ctx->npercpu = npercpu;
            ctx->bsize = _PMR_BLOCKLEN_MERGE;
            ctx->sort_effector = _(_PMR_PRESORT);
            ctx->merge_effector = _(aux_merge);

            /* run parallel sort */
            return _(pmergesort_impl)(ctx);
        }
    }
#endif

#if (PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS) && _PMR_PARALLEL_MAY_SPAWN
    ctx->thpool = NULL; /* disable threads spawn */
#endif

    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    size_t bsz = _PMR_BLOCKLEN_MERGE;

    void * a = lo;
    void * b = ELT_PTR_FWD(ctx, a, bsz);

    while (b <= hi)
    {
        _(_PMR_PRESORT)(a, a, b, ctx, NULL);

        a = b;
        b = ELT_PTR_FWD(ctx, b, bsz);
    }

    _(_PMR_PRESORT)(a, a, hi, ctx, NULL);

    aux_t aux;
    memset(&aux, 0, sizeof(aux));

    while (bsz < ctx->n)
    {
        size_t bsz1 = bsz << 1;

        a = lo;
        b = ELT_PTR_FWD(ctx, a, bsz1);

        while (b <= hi)
        {
            _(aux_merge)(a, ELT_PTR_FWD(ctx, a, bsz), b, ctx, &aux);
            if (aux.rc != 0)
                goto bail_out;

            a = b;
            b = ELT_PTR_FWD(ctx, b, bsz1);
        }

        _(aux_merge)(a, ELT_PTR_FWD(ctx, a, bsz), hi, ctx, &aux);
        if (aux.rc != 0)
            goto bail_out;

        bsz = bsz1;
    }

bail_out:;
    _aux_free(&aux);

    return aux.rc;
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  compound mergesort (symmerge based) with wrapped pre-sort function                                                        */
/* -------------------------------------------------------------------------------------------------------------------------- */

static __attribute__((unused)) void _(wrap_sort)(void * lo, __unused void * mi, void * hi, context_t * ctx, aux_t * aux)
{
    aux->rc = CALL_SORT(ctx, lo, ELT_DIST(ctx, hi, lo)); /* FIXME: atomic */
}

static inline int _(wrapmergesort)(context_t * ctx)
{
#if PMR_PARALLEL_USE_GCD || PMR_PARALLEL_USE_PTHREADS || PMR_PARALLEL_USE_OMP
    if (ctx->n >= 2 * _PMR_BLOCKLEN_MTHRESHOLD0 * _PMR_BLOCKLEN_SYMMERGE)
    {
        for (int ncpu = ctx->ncpu; ncpu > 1; ncpu--)
        {
            size_t npercpu = IDIV_UP(ctx->n, ncpu);
            if (npercpu >= 2 * _PMR_BLOCKLEN_MTHRESHOLD * _PMR_BLOCKLEN_SYMMERGE)
            {
                /* use parallel when have a long enough array could be distributed by cores */

                /* pre-set initial pass values */
                ctx->npercpu = npercpu;
                ctx->bsize = npercpu;
                ctx->sort_effector = _(wrap_sort);
                ctx->merge_effector = _(aux_symmerge);

#if PMR_PARALLEL_USE_OMP && _PMR_PARALLEL_MAY_SPAWN
                thr_pool_t pool;
                pool.ncpu = ctx->ncpu; /* utilize maximum number of cores when symmerge spawns threads */

                ctx->thpool = &pool;
#endif

                /* run parallel sort */
                return _(pmergesort_impl)(ctx);
            }
        }
    }
#endif

    /* fallback to wrapped sort function */

    return CALL_SORT(ctx, (void *)ctx->base, ctx->n);
}

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  low efficient inplace sorters for profiling purposes                                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_CORE_PROFILE
static inline void _(insertionsort)(context_t * ctx)
{
    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    _(binsort)(lo, lo, hi, ctx, NULL);
}
#endif

#if _PMR_CORE_PROFILE
static inline void _(insertionsort_run)(context_t * ctx)
{
    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    _(binsort_run)(lo, lo, hi, ctx, NULL);
}
#endif

#if _PMR_CORE_PROFILE
static inline void _(insertionsort_mergerun)(context_t * ctx)
{
    void * lo = (void *)ctx->base;
    void * hi = ELT_PTR_FWD(ctx, lo, ctx->n);

    _(binsort_mergerun)(lo, lo, hi, ctx, NULL);
}
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
