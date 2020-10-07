//
// Created by Mariano Sorgente on 2020/09/27.
//

#ifndef CHIAPOS_THREADING_HPP
#define CHIAPOS_THREADING_HPP

#ifdef _WIN32
#elif __APPLE__
#include <unistd.h>
#include <dispatch/dispatch.h>
#else
#include <unistd.h>
#include <semaphore.h>
#endif


class SemaphoreUtils {
public:
#ifdef _WIN32
    static inline void Wait(HANDLE* semaphore) { WaitForSingleObject(*semaphore, INFINITE); }
#elif __APPLE__
    static inline void Wait(dispatch_semaphore_t *semaphore) {
        dispatch_semaphore_wait(*semaphore, DISPATCH_TIME_FOREVER);
    }
#else
    static inline void Wait(sem_t *semaphore) { sem_wait(semaphore); }
#endif

#ifdef _WIN32
    static inline void Post(HANDLE* semaphore) { ReleaseSemaphore(*semaphore, 1, NULL); }
#elif __APPLE__

    static inline void Post(dispatch_semaphore_t *semaphore) {
        dispatch_semaphore_signal(*semaphore);
    }
#else
    static inline void Post(sem_t *semaphore) { sem_post(semaphore); }
#endif

};

//        std::cout << ptd->index << " waited 0" << std::endl;
#endif  // CHIAPOS_THREADING_HPP
