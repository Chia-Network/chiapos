#include <stdio.h>
#include "verifier.hpp"

#include <sstream>
#include <string>
#include <fstream>
#include <thread>

std::vector<uint8_t> HexToBytes(const char *hex_proof) {
    int len = strlen(hex_proof);
    assert(len % 2 == 0);
    std::vector<uint8_t> result;
    for (int i = 0; i < len; i += 2)
    {
        int hex1 = hex_proof[i] >= 'a' ? (hex_proof[i] - 'a' + 10) : (hex_proof[i] - '0');
        int hex2 = hex_proof[i + 1] >= 'a' ? (hex_proof[i + 1] - 'a' + 10) : (hex_proof[i + 1] - '0');
        result.push_back(hex1 * 16 + hex2);
    }
    return result;
}

void doit(int thread)
{
    std::ifstream infile("pos.txt");

    std::string plot_id;
    std::string pos_size;
    std::string pos_challenge;
    std::string pos_proof;

    int cnt=0;

    while(true)
    {
        std::getline(infile, plot_id);
        if (infile.eof())
            break;
        std::getline(infile, pos_size);
        std::getline(infile, pos_challenge);
        std::getline(infile, pos_proof);

        if(cnt%10==thread)
        {
            bool isok=false;

            std::vector<uint8_t> plotidbytes=HexToBytes(plot_id.c_str());
            std::vector<uint8_t> challengebytes=HexToBytes(pos_challenge.c_str());
            std::vector<uint8_t> proofbytes=HexToBytes(pos_proof.c_str());

            char *endptr;

            int posk=strtoll(pos_size.c_str(),&endptr,10);

            Verifier verifier = Verifier();
            LargeBits result = verifier.ValidateProof(plotidbytes.data(), posk, challengebytes.data(), proofbytes.data(), proofbytes.size());
            isok=result.GetSize()!=0;
 
            printf("thread %d cnt %d is valid %d %s %s\n",thread,cnt,isok,plot_id.c_str(),pos_challenge.c_str());
        }
        cnt++;
    }
}

int main()
{
    std::vector<std::jthread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back(doit, i);
    }
    doit(9);

    return 0;
}
