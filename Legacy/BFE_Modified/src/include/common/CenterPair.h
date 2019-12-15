/*
Flock Evaluation Algorithms

Copyright (C) 2009 Marcos R. Vieira
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

#ifndef FLOCKS_CENTERPAIR_H
#define FLOCKS_CENTERPAIR_H

#include "Center.h"

namespace flockcommon {
    class CenterPair {
    public:
        void operator=(CenterPair c);

        bool operator<(CenterPair c) const;

        Center get(int idx);

        Center & operator[](int idx);

        bool valid = false;
        Center centers[2];
    };

}

#endif //FLOCKS_CENTERPAIR_H
