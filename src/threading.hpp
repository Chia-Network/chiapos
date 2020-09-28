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
    static inline void Wait(sem_t* semaphore) {
#ifdef _WIN32
        WaitForSingleObject(semaphore, INFINITE);
#else
        sem_wait(semaphore);
#endif
    }

    static inline void Post(sem_t* semaphore) {
#ifdef _WIN32
        ReleaseSemaphore(semaphore, 1, NULL);
#else
        sem_post(semaphore);
#endif
    }

};

//        std::cout << ptd->index << " waited 0" << std::endl;
#endif //CHIAPOS_THREADING_HPP
