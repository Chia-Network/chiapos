// num.cpp
#include "nummer.hpp"
#include "num.h"

Num NumInit() {
    cxxNum * ret = new cxxNum(1);
    return (void*)ret;
}

void NumFree(Num n) {
    cxxNum * num = (cxxNum*)n;
    delete num;
}

void NumIncrement(Num n) {
    cxxNum * num = (cxxNum*)n;
    num->Increment();
}

int NumGetValue(Num n) {
    cxxNum * num = (cxxNum*)n;
    return num->GetValue();
}