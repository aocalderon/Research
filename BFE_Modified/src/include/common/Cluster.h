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

#ifndef FLOCKS_CLUSTER_H
#define FLOCKS_CLUSTER_H


#include "PointSet.h"
#include <vector>

namespace flockcommon {
    class Cluster {
    public:

        Cluster() { };

        Cluster(const unsigned int time) : endTime(time), startTime(time) {};

        Cluster(const PointSet &points) : points(points) { }


        static unsigned int countIntersection(PointSet pnts1, PointSet pnts2);

        static PointSet setIntersection(PointSet &, PointSet &);

        static void insertCluster(vector<PointSet> &clusters, PointSet &pnts);

        static void insertNewFlock(vector<Cluster> &clusters, Cluster newClt);


        PointSet points;
        unsigned int endTime = 0;
        unsigned int startTime = 0;
    };
}


#endif //FLOCKS_CLUSTER_H
