//
// Created by Mariano Sorgente on 2020/09/27.
//

#ifndef CHIAPOS_THREADING_HPP
#define CHIAPOS_THREADING_HPP

#ifndef _WIN32
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#endif

class SemaphoreUtils {
public:

#ifdef _WIN32
    static inline void Wait(HANDLE semaphore)
        WaitForSingleObject(semaphore, INFINITE);
#else

    static inline void Wait(sem_t *semaphore) {
        sem_wait(semaphore);
    }

#endif

#ifdef _WIN32
    static inline void Post(HANDLE semaphore)
    {
        ReleaseSemaphore(semaphore, 1, NULL);
#else

    static inline void Post(sem_t *semaphore)
    {
        sem_post(semaphore);
#endif
    }
};

//        std::cout << ptd->index << " waited 0" << std::endl;
#endif  // CHIAPOS_THREADING_HPP
