/* -------------------------------------------------------------------------------------------------------------------------- */
/*  pmergesort-mem-n.inl                                                                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */
/*  Created by Cyril Murzin                                                                                                   */
/*  Copyright (c) 2015-2017 Ravel Developers Group. All rights reserved.                                                      */
/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  swap two elements of size                                                                                                 */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(swap)(void * a, void * b, size_t sz)
{
    ELT_TYPE t = *(ELT_TYPE *)a;
    *(ELT_TYPE *)a = *(ELT_TYPE *)b;
    *(ELT_TYPE *)b = t;
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  copy n elements of size                                                                                                   */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(copy)(void * src, void * dst, size_t n, size_t sz)
{
    ELT_TYPE * p = src;
    ELT_TYPE * q = dst;

    switch (n)
    {
    case 8: *q++ = *p++;
    case 7: *q++ = *p++;
    case 6: *q++ = *p++;
    case 5: *q++ = *p++;
    case 4: *q++ = *p++;
    case 3: *q++ = *p++;
    case 2: *q++ = *p++;
    case 1: *q++ = *p++;
    case 0:
        break;
    default:
        _region_copy(p, q, ELT_OF_SZ(n, sz));
        break;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  move n elements of size sz at base a to the right by m elements (of size sz)                                              */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(move_right)(void * a, size_t n, size_t m, size_t sz)
{
    if (n <= 8)
    {
        ELT_TYPE * src = ELT_PTR_FWD_(a, n - 1, sz);
        ELT_TYPE * dst = ELT_PTR_FWD_(src, m, sz);

        switch (n)
        {
        case 8: *dst-- = *src--;
        case 7: *dst-- = *src--;
        case 6: *dst-- = *src--;
        case 5: *dst-- = *src--;
        case 4: *dst-- = *src--;
        case 3: *dst-- = *src--;
        case 2: *dst-- = *src--;
        case 1: *dst-- = *src--;
        case 0:
        default:
            break;
        }
    }
    else
        _region_move_right(a, ELT_PTR_FWD_(a, m, sz), ELT_OF_SZ(n, sz));
}

/* -------------------------------------------------------------------------------------------------------------------------- */
/*  move n elements of size sz at base a to the left by m elements (of size sz)                                               */
/* -------------------------------------------------------------------------------------------------------------------------- */
static inline void _M(move_left)(void * a, size_t n, size_t m, size_t sz)
{
    ELT_TYPE * src = a;
    ELT_TYPE * dst = ELT_PTR_BCK_(a, m, sz);

    switch (n)
    {
    case 8: *dst++ = *src++;
    case 7: *dst++ = *src++;
    case 6: *dst++ = *src++;
    case 5: *dst++ = *src++;
    case 4: *dst++ = *src++;
    case 3: *dst++ = *src++;
    case 2: *dst++ = *src++;
    case 1: *dst++ = *src++;
    case 0:
        break;
    default:
        _region_move_left(src, dst, ELT_OF_SZ(n, sz));
        break;
    }
}

/* -------------------------------------------------------------------------------------------------------------------------- */
