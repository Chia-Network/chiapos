//
// Created by Mariano Sorgente on 2020/09/27.
//

#ifndef CHIAPOS_THREADING_HPP
#define CHIAPOS_THREADING_HPP

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define VC_EXTRALEAN
#define STRICT
#include <windows.h>
#elif __APPLE__
#include <unistd.h>
#include <dispatch/dispatch.h>
#else
#include <unistd.h>
#include <semaphore.h>
#endif

// TODO: in C++20, this can be replaced with std::binary_semaphore
namespace Sem {
#ifdef _WIN32
    using type = HANDLE;
    inline void Wait(HANDLE* semaphore) { WaitForSingleObject(*semaphore, INFINITE); }
    inline void Post(HANDLE* semaphore) { ReleaseSemaphore(*semaphore, 1, NULL); }
    inline HANDLE Create() {
        return CreateSemaphore(
            nullptr,   // default security attributes
            0,      // initial count
            2,      // maximum count
            nullptr);  // unnamed semaphore
    }
    inline void Destroy(HANDLE sem) {
        CloseHandle(sem);
    }
#elif __APPLE__
    using type = dispatch_semaphore_t;
    inline void Wait(dispatch_semaphore_t *semaphore) {
        dispatch_semaphore_wait(*semaphore, DISPATCH_TIME_FOREVER);
    }
    inline void Post(dispatch_semaphore_t *semaphore) {
        dispatch_semaphore_signal(*semaphore);
    }
    inline dispatch_semaphore_t Create() {
        return dispatch_semaphore_create(0);
    }
    inline void Destroy(dispatch_semaphore_t sem) {
        dispatch_release(sem);
    }
#else
    using type = sem_t;
    inline void Wait(sem_t *semaphore) { sem_wait(semaphore); }
    inline void Post(sem_t *semaphore) { sem_post(semaphore); }
    inline sem_t Create() {
        sem_t ret;
        sem_init(&ret, 0, 0);
        return ret;
    }
    inline void Destroy(sem_t& sem) {
        sem_close(&sem);
    }
#endif

};

//        std::cout << ptd->index << " waited 0" << std::endl;
#endif  // CHIAPOS_THREADING_HPP
