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

#ifndef FLOCKS_POINT_H
#define FLOCKS_POINT_H

#include "definitions.hpp"
#include "string"
#include <set>

using namespace std;

namespace flockcommon {


    enum class DistanceMetric {
        L1, // Manhattan
        L2, // Euclidean
        CAN, // Canberra
        INF // L-infinity
    };

//------------------------------------------------------------------------------
    class Point {
    public:

        Point() {};

        Point(double x, double y) : x(x), y(y) { this->oid = 0; }

        Point(double x, double y, unsigned int oid) : x(x), y(y), oid(oid) { }


        void operator=(const Point &p);

        bool operator==(const Point &p) const;

        bool operator!=(const Point &p) const;

        bool operator<(Point p) const;

        bool isInRangeQueryWith(const Point &point, double &range) const;

        string serialize() const;
        double distance(const Point &p, const DistanceMetric &metric = DistanceMetric::L2) const;

        set<Point, less<Point>>* getNeighborhood(const set<Point, less<Point>> &pnts, const
        double &distThreshold) const;

        double x = 0;
        double y = 0;
        unsigned int oid = 0;
    };


}
#endif //FLOCKS_POINT_H
