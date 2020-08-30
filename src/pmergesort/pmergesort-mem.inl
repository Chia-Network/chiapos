/* -------------------------------------------------------------------------------------------------------------------------- */
/*  pmergesort-mem.inl                                                                                                        */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  Created by Cyril Murzin                                                                                                   */
/*  Copyright (c) 2015-2017 Ravel Developers Group. All rights reserved.                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  swaps two segments of the same size                                                                                       */
/*  [a, a+sz*(n-1)] <=> [b, b+sz*(n-1)]                                                                                       */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(swap_r)(void * a, void * b, size_t n, size_t sz)
{
    _regions_swap(a, b, sz * n);
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  rev[lo, mid]                                                                                                              */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(reverse)(void * lo, void * hi, size_t sz)
{
    while (lo < hi)
    {
        _M(swap)(lo, hi, sz);
        lo = ELT_PTR_FWD_(lo, 1, sz);
        hi = ELT_PTR_BCK_(hi, 1, sz);
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  consider rotate as swap of two consecutive segments of the vary sizes                                                     */
/*  [lo, mid) <=> [mid, hi)                                                                                                   */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(rotate)(void * lo, void * mid, void * hi, size_t sz)
{
    if (lo < mid && mid < hi)
    {
        size_t i = ELT_DIST_(mid, lo, sz);
        size_t j = ELT_DIST_(hi, mid, sz);

        if (i > j && j <= _PMR_TMP_ROT)
        {
            /* up to _PMR_TMP_ROT temp values to put at stack temporary storage */

            uint8_t t[ELT_OF_SZ(_PMR_TMP_ROT, sz)];

            _M(copy)(mid, t, j, sz);
            _M(move_right)(lo, i, j, sz);
            _M(copy)(t, lo, j, sz);
        }
        else if (j >= i && i <= _PMR_TMP_ROT)
        {
            /* up to _PMR_TMP_ROT temp values to put at stack temporary storage */

            uint8_t t[ELT_OF_SZ(_PMR_TMP_ROT, sz)];

            _M(copy)(lo, t, i, sz);
            _M(move_left)(mid, j, i, sz);
            _M(copy)(t, ELT_PTR_BCK_(hi, i, sz), i, sz);
        }
        else
        {
            /* straight rotate with regions swaps (fastest) */

            while (i != j)
            {
                if (i > j)
                {
                    _M(swap_r)(ELT_PTR_BCK_(mid, i, sz), mid, j, sz);
                    i -= j;
                }
                else
                {
                    _M(swap_r)(ELT_PTR_BCK_(mid, i, sz), ELT_PTR_FWD_(mid, j - i, sz), i, sz);
                    j -= i;
                }
            }

            if (i > 0)
                _M(swap_r)(ELT_PTR_BCK_(mid, i, sz), mid, i, sz);
        }
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  consider rotate as swap of two consecutive segments of the vary sizes (uses aux memory)                                   */
/*  [lo, mid) <=> [mid, hi)                                                                                                   */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(rotate_aux)(void * lo, void * mid, void * hi, size_t sz, aux_t * aux)
{
    if (lo < mid && mid < hi)
    {
        size_t i = ELT_DIST_(mid, lo, sz);
        size_t j = ELT_DIST_(hi, mid, sz);

        if (i > j)
        {
            if (j <= _PMR_TMP_ROT)
            {
                /* up to _PMR_TMP_ROT temp values to put at stack temporary storage */

                uint8_t t[ELT_OF_SZ(_PMR_TMP_ROT, sz)];

                _M(copy)(mid, t, j, sz);
                _M(move_right)(lo, i, j, sz);
                _M(copy)(t, lo, j, sz);
            }
            else
            {
                /* allocate or adjust size of temporary storage if needed, caller will free it */
                void * tmp = _aux_alloc(aux, ELT_OF_SZ(j, sz));
                if (tmp == NULL)
                    return; /* bail out due to not enough memory error */

                _M(copy)(mid, tmp, j, sz);
                _M(move_right)(lo, i, j, sz);
                _M(copy)(tmp, lo, j, sz);
            }
        }
        else /* j >= i */
        {
            if (i <= _PMR_TMP_ROT)
            {
                /* up to _PMR_TMP_ROT temp values to put at stack temporary storage */

                uint8_t t[ELT_OF_SZ(_PMR_TMP_ROT, sz)];

                _M(copy)(lo, t, i, sz);
                _M(move_left)(mid, j, i, sz);
                _M(copy)(t, ELT_PTR_BCK_(hi, i, sz), i, sz);
            }
            else
            {
                /* allocate or adjust size of temporary storage if needed, caller will free it */
                void * tmp = _aux_alloc(aux, ELT_OF_SZ(i, sz));
                if (tmp == NULL)
                    return; /* bail out due to not enough memory error */

                _M(copy)(lo, tmp, i, sz);
                _M(move_left)(mid, j, i, sz);
                _M(copy)(tmp, ELT_PTR_BCK_(hi, i, sz), i, sz);
            }
        }
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
