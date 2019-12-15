/*
Flock Evaluation Algorithms

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

#ifndef FLOCKS_PLANESWEEP_H
#define FLOCKS_PLANESWEEP_H
//#define PS_DEBUG
//#define PS_STEPS_DBG



#endif //FLOCKS_PLANESWEEP_H

#include <algorithm>
#include <ctime>
#include <common/Cluster.h>
#include "PlaneBox.h"
#include "HashPlaneBox.h"

using namespace std;
using namespace flockcommon;

extern bool point_x_compare(Point, Point);
extern bool box_oid_compare(PlaneBox, PlaneBox);
extern bool box_x_compare(PlaneBox, PlaneBox);

/**
 * Class PlaneSweep.
 * This class performs a search in the plane for valid
 */
class PlaneSweep {
public:

    PlaneSweep(const double &distance, const unsigned int flockSize, const set<Point> &points) :
            distance(distance), points(points), flockSize(flockSize), boxes() {}

    PlaneSweep(const double &distance, const unsigned int flockSize, const PointSet &points) :
            distance(distance), points(points.points), flockSize(flockSize), boxes() {}


    vector<PlaneBox> getLiveBoxes();

    vector<HashCluster> getValidHashClusters();

    vector<PointSet> getValidClusters();

    unsigned long getCenters() const;

    double getTimeSweepClustering() const;

    double getTimeBoxes() const;

    unsigned long getPairs() const;

    unsigned long getClusterCount() const;

private:

    double distance = 0;
    double timeBoxClustering = 0;
    double timeBoxes = 0;
    double timeSweepClustering = 0;
    set<Point, less<Point>> points;
    unsigned int flockSize = 0;
    unsigned long centers = 0;
    unsigned long clusterCount = 0;
    unsigned long pairs = 0;

    vector<PlaneBox> boxes;

    template <class TBox>
    TBox calculateBox(vector<Point>::iterator, vector<Point>::iterator, vector<Point>&);

};



