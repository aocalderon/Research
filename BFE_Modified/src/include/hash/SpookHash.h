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

#ifndef PSBHASHES_SPOOKYHASH_H
#define PSBHASHES_SPOOKYHASH_H

#include "Hash.h"
#include "SpookyV2.h"
#include <iostream>


class SpookHash : public Hash {


public:
    SpookHash() {
        this->name = "SpookyHash";
    };

    bitset<SIGN_SIZE> hash(const void *buffer, unsigned int length);

    u_int32_t pureHash(const void *buffer, unsigned int length, int seed);
};


#endif //PSBHASHES_SPOOKYHASH_H

