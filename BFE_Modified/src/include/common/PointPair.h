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

#ifndef FLOCKS_POINTPAIR_H
#define FLOCKS_POINTPAIR_H

#include "CenterPair.h"
#include "Point.h"


namespace flockcommon {
    class PointPair {
    public:

        PointPair operator=(PointPair p);

        bool operator<(PointPair p) const;

        Point get(int idx);

        Point &operator[](int idx);

        CenterPair calculateCenters(double radius);

        Point points[2];
    };
}


#endif //FLOCKS_POINTPAIR_H
