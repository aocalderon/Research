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

#ifndef PSBHASHES_MURMURHASH_H
#define PSBHASHES_MURMURHASH_H

#define DEBUG_MURMUR

#include "Hash.h"
#include "murmur3.h"
#include <iostream>
#include <cmath>


class MurMurHash : public Hash {


public:
    MurMurHash() {
        this->name = "MurMur3";
    }

    bitset<SIGN_SIZE> hash(const void *buffer, unsigned int length);

    u_int32_t pureHash(const void *, unsigned int, int seed);

    bitset<SIGN_SIZE> hashInt(const void *buffer, unsigned int length, u_int64_t seed, int iterations);
};


#endif //PSBHASHES_MURMURHASH_H


