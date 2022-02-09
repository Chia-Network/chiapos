class cxxNum {
private:
    int value;

public:
    cxxNum(int _num) : value(_num){};
    ~cxxNum(){};
    void Increment(void);
    int GetValue(void);
};