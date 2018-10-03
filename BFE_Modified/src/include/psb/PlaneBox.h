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

#ifndef FLOCKS_PLANEBOX_H
#define FLOCKS_PLANEBOX_H

#include <common/Cluster.h>
#include <unordered_set>
#include "HashCluster.h"

using namespace flockcommon;

class PlaneBox {
    friend class PlaneSweep;

public:

    PlaneBox(const Point &point, const double &range = 1.0);

    virtual void addPoint(const Point &point);

    unsigned long getPointCount();

    vector<HashCluster> getValidHashClusters(const unsigned int  flockSize);

    vector<PointSet> getValidClusters(const unsigned int);

    bool intersectsWith(const PlaneBox &box);

    unsigned long getCenters() const;

    unsigned long getPairs() const;

    const Point & getNwe() const;

    const Point & getSea() const;

    bool areCenterPointsCloseEnoughInXWith(const PlaneBox& planeBox) const;

    bool hasPoint(const Point);


    string toString();


    Point getReferencePoint() const {
        return this->point;
    }

    bool operator==(const PlaneBox &other) {
        return this->point.oid == other.getReferencePoint().oid;
    }

    vector<Point> points;

    bool isMember(unsigned int oid);


    const unordered_set<unsigned int> &getMembership() const {
        return membership;
    }

protected:
    Point point;
    vector< HashCluster > validClusters;
    vector< PointSet > clusters;

    virtual void clear();

private:
    unordered_set<unsigned int> membership;
    Point nwe;
    Point sea;

    double range = 0;
    unsigned int leftPoints = 0;
    unsigned int rigthPoints = 0;
    unsigned long centers = 0;
    unsigned long endTime = 0;
    unsigned long pairs = 0;
    unsigned long startTime = 0;

    void resetMbr();

    void expand(const Point &point);
};


#endif //FLOCKS_PLANEBOX_H


