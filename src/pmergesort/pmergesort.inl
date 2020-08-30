/* -------------------------------------------------------------------------------------------------------------------------- */
/*  pmergesort.inl                                                                                                            */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  Created by Cyril Murzin                                                                                                   */
/*  Copyright (c) 2015-2017 Ravel Developers Group. All rights reserved.                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_USE_4_MEM

#define SORT_SUFFIX                 4

#define ELT_SZ(ctx)                 4
#define ELT_OF_SZ(n, sz)            ((n) << 2)
#define ELT_PTR_FWD_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) + (__inx << 2); })
#define ELT_PTR_BCK_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) - (__inx << 2); })
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) >> 2)

#include "pmergesort-core.inl"

#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ
#undef ELT_SZ

#undef SORT_SUFFIX

#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_USE_8_MEM

#define SORT_SUFFIX                 8

#define ELT_SZ(ctx)                 8
#define ELT_OF_SZ(n, sz)            ((n) << 3)
#define ELT_PTR_FWD_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) + (__inx << 3); })
#define ELT_PTR_BCK_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) - (__inx << 3); })
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) >> 3)

#include "pmergesort-core.inl"

#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ
#undef ELT_SZ

#undef SORT_SUFFIX

#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_USE_16_MEM

#define SORT_SUFFIX                 16

#define ELT_SZ(ctx)                 16
#define ELT_OF_SZ(n, sz)            ((n) << 4)
#define ELT_PTR_FWD_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) + (__inx << 4); })
#define ELT_PTR_BCK_(base, inx, sz) ({ __typeof__(inx) __inx = (inx); ((void *)(base)) - (__inx << 4); })
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) >> 4)

#include "pmergesort-core.inl"

#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ
#undef ELT_SZ

#undef SORT_SUFFIX

#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#define SORT_SUFFIX                 sz

#define ELT_SZ(ctx)                 (ctx)->sz
#define ELT_OF_SZ(n, sz)            ((sz) * (n))
#define ELT_PTR_FWD_(base, inx, sz) (((void *)(base)) + (sz) * (inx))
#define ELT_PTR_BCK_(base, inx, sz) (((void *)(base)) - (sz) * (inx))
#define ELT_DIST_(a, b, sz)         ((((void *)(a)) - ((void *)(b))) / (sz))

#include "pmergesort-core.inl"

#undef ELT_DIST_
#undef ELT_PTR_FWD_
#undef ELT_PTR_BCK_
#undef ELT_OF_SZ
#undef ELT_SZ

#undef SORT_SUFFIX

/* -------------------------------------------------------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------------------------------------------------------- */

static inline void _F(symmergesort)(context_t * ctx)
{
    switch (ctx->sz)
    {
#if _PMR_USE_4_MEM
    case 4:
        _F(_symmergesort_4)(ctx);
        break;
#endif
#if _PMR_USE_8_MEM
    case 8:
        _F(_symmergesort_8)(ctx);
        break;
#endif
#if _PMR_USE_16_MEM
    case 16:
        _F(_symmergesort_16)(ctx);
        break;
#endif
    default:
        _F(_symmergesort_sz)(ctx);
        break;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

static inline int _F(pmergesort)(context_t * ctx)
{
    switch (ctx->sz)
    {
#if _PMR_USE_4_MEM
    case 4:
        return _F(_pmergesort_4)(ctx);
#endif
#if _PMR_USE_8_MEM
    case 8:
        return _F(_pmergesort_8)(ctx);
#endif
#if _PMR_USE_16_MEM
    case 16:
        return _F(_pmergesort_16)(ctx);
#endif
    default:
        return _F(_pmergesort_sz)(ctx);
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

static inline int _F(wrapmergesort)(context_t * ctx)
{
    switch (ctx->sz)
    {
#if _PMR_USE_4_MEM
    case 4:
        return _F(_wrapmergesort_4)(ctx);
#endif
#if _PMR_USE_8_MEM
    case 8:
        return _F(_wrapmergesort_8)(ctx);
#endif
#if _PMR_USE_16_MEM
    case 16:
        return _F(_wrapmergesort_16)(ctx);
#endif
    default:
        return _F(_wrapmergesort_sz)(ctx);
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_CORE_PROFILE
static inline void _F(insertionsort)(context_t * ctx)
{
    switch (ctx->sz)
    {
#if _PMR_USE_4_MEM
    case 4:
        _F(_insertionsort_4)(ctx);
        break;
#endif
#if _PMR_USE_8_MEM
    case 8:
        _F(_insertionsort_8)(ctx);
        break;
#endif
#if _PMR_USE_16_MEM
    case 16:
        _F(_insertionsort_16)(ctx);
        break;
#endif
    default:
        _F(_insertionsort_sz)(ctx);
        break;
    }
}
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_CORE_PROFILE
static inline void _F(insertionsort_run)(context_t * ctx)
{
    switch (ctx->sz)
    {
#if _PMR_USE_4_MEM
    case 4:
        _F(_insertionsort_run_4)(ctx);
        break;
#endif
#if _PMR_USE_8_MEM
    case 8:
        _F(_insertionsort_run_8)(ctx);
        break;
#endif
#if _PMR_USE_16_MEM
    case 16:
        _F(_insertionsort_run_16)(ctx);
        break;
#endif
    default:
        _F(_insertionsort_run_sz)(ctx);
        break;
    }
}
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */

#if _PMR_CORE_PROFILE
static inline void _F(insertionsort_mergerun)(context_t * ctx)
{
    switch (ctx->sz)
    {
#if _PMR_USE_4_MEM
    case 4:
        _F(_insertionsort_mergerun_4)(ctx);
        break;
#endif
#if _PMR_USE_8_MEM
    case 8:
        _F(_insertionsort_mergerun_8)(ctx);
        break;
#endif
#if _PMR_USE_16_MEM
    case 16:
        _F(_insertionsort_mergerun_16)(ctx);
        break;
#endif
    default:
        _F(_insertionsort_mergerun_sz)(ctx);
        break;
    }
}
#endif

/* -------------------------------------------------------------------------------------------------------------------------- */
