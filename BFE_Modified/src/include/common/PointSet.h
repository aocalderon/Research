/*
Flock Evaluation Algorithms

Copyright (C) 2009 Marcos R. Vieira
Copyright (C) 2015, 2016 Pedro S. Tanaka
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

#ifndef FLOCKS_POINTSET_H
#define FLOCKS_POINTSET_H

#include <set>
#include <algorithm>
#include "Point.h"
#include "Center.h"

using namespace std;

bool point_oid_cmp(const flockcommon::Point &a, const flockcommon::Point &b);

namespace flockcommon {
    class PointSet {
    public:
        typedef set<Point, less<Point> > _pointSet;
        typedef set<Point, less<Point> >::iterator my_iter;
        typedef set<Point, less<Point> >::const_iterator my_const_iter;


        PointSet() {};

        PointSet(Center c, double r) {
            points.clear();
            centerX = c.x;
            centerY = c.y;
            radius = r;
        }

        PointSet(set<Point> points) : points (points) {};

        PointSet operator=(PointSet);

        set<::flockcommon::Point>::const_iterator cbegin() const;

        set<::flockcommon::Point>::const_iterator cend() const;

        void invalidateCenter();

        void setCenter(Center c, double r);

        void clear();

        bool usePruning();

        _pointSet::iterator begin() {
            return points.begin();
        }

        _pointSet::iterator end() {
            return points.end();
        }

        virtual void insert(const Point p);

        void insert(const my_iter it1, const my_iter it2);

        unsigned int size();

        bool empty() const;

        string toString();

        /**
         * Check if a point set is contained in other
         */
        bool contains(PointSet other);

        bool hasPoint(const Point point);

        bool inRange(Point point, DistanceMetric metric = DistanceMetric::L2);

        string getOids(char delimiter = '|');

        _pointSet points;
        bool pruning = true;
        double centerX = 0;
        double centerY = 0;
        double radius = 0;
        unsigned int time = 0;

    };
}


#endif //FLOCKS_POINTSET_H
