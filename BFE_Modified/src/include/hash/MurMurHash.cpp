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

#include "MurMurHash.h"


using namespace std;


bitset<SIGN_SIZE> MurMurHash::hash(const void *buffer, const unsigned int length) {
    u_int32_t hash[4], seed = 3;

    MurmurHash3_x64_128(buffer, length, seed, hash);

    #ifdef DEBUG_MURMUR
    cout << endl << "\tMurMur3: ";

    for (unsigned  short i = 0; i < 4; ++i) {
        cout << hash[i];

        if (i != 3) cout << ", ";
    }

    #endif

    bitset<SIGN_SIZE> sign = hashToBitset(hash);

    #ifdef DEBUG_MURMUR
        cout << "\t" << sign << endl;
    #endif

    return sign;
}

bitset<SIGN_SIZE> MurMurHash::hashInt(const void *buffer, const unsigned int length,
                                      u_int64_t seed = 0, const int iterations = 5) {

    u_int32_t hash[4];

    MurmurHash3_x64_128(buffer, length, seed, hash);
    MurmurHash3_x64_128(buffer, length, hash[1], hash);

    bitset<SIGN_SIZE> hashs;

    for (int i = 0; i < iterations; ++i) {
        unsigned int bucketPos = (hash[0] + i * hash[1]) % SIGN_SIZE;
        hashs.set((size_t) bucketPos);
    }

    return hashs;
}

u_int32_t MurMurHash::pureHash(const void * buffer, const unsigned int length, int seed) {
    u_int32_t hash[4];

    MurmurHash3_x64_128(buffer, length, 0, hash);

    return hash[1];
}
