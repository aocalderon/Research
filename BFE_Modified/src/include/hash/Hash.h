/*
Flock Evaluation Algorithms

Copyright (C) 2015 Pedro S. Tanaka
Copyright (C) 2016 Denis E. Sanches

This program is free software: you can redistribute it and/or modify
        it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
        but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef PSBHASHES_HASH_H
#define PSBHASHES_HASH_H

#include <string>
#include <bitset>
#include <climits>
#include <cstdint>

#define SIGN_SIZE 128
//#define FHASH_DEBUG

using namespace std;


class Hash {
public:
    Hash() { };

    virtual bitset<SIGN_SIZE> hash(const void *buffer, unsigned int length) = 0;

    virtual u_int32_t pureHash(const void *buffer, unsigned int length, int seed) = 0;


    const string &getName() const {
        return name;
    }

protected:

    std::bitset<SIGN_SIZE> hashToBitset(u_int32_t *data) {
        //FIXME: make this method generalist, using void pointers and passing a length
        std::bitset<SIGN_SIZE> b;


        int intBits = (sizeof(u_int32_t) * CHAR_BIT);
        for (int i = 0; i < (SIGN_SIZE / intBits); ++i) {
            u_int32_t cur = data[i];
            int offset = i * (intBits);

            for (int bit = 0; bit < intBits; ++bit) {
                int mask = 1 << bit;
                int maskedN = cur & mask;
                b[offset] = maskedN >> bit;
                ++offset;   // Move to next bit in b
            }
        }

        return b;
    }

    string name;
};

#endif //PSBHASHES_HASH_H
