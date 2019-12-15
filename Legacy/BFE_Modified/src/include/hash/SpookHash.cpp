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

#include "SpookHash.h"

using namespace std;

bitset<SIGN_SIZE> SpookHash::hash(const void *buffer, unsigned int length) {
    uint64 hash1 = 0, hash2 = 0;
    SpookyHash::Hash128(buffer, length, &hash1, &hash2);

    bitset<SIGN_SIZE> partial((unsigned long) hash1);

    if (SIGN_SIZE > 64) {
        partial <<= 64;
        bitset<SIGN_SIZE> partial2((unsigned long)  hash2);
        partial |= partial2;
    }

    cout << partial  << endl;

    return partial;
}

u_int32_t SpookHash::pureHash(const void *buffer, unsigned int length, int seed) {
    return SpookyHash::Hash32(buffer, length, (uint32) seed);
}
