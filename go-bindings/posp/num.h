// num.h
#ifdef __cplusplus
extern "C" {
#endif

typedef void* Num;
Num NumInit(void);
void NumFree(Num);
void NumIncrement(Num);
int NumGetValue(Num);

#ifdef __cplusplus
}
#endif