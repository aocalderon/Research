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

#include "HashPlaneBox.h"
#include "PointPair.h"

using namespace flockcommon;

// #FIXME This creates an Box with getPointCount() == 0, what can be dangerous...
PlaneBox::PlaneBox(const Point &point, const double &range) :
        point(point), nwe(point), sea(point), range(range) {

    this->membership.insert(point.oid);
}

vector<HashCluster> PlaneBox::getValidHashClusters(const unsigned int  flockSize) {
    if (this->validClusters.size() > 0) return this->validClusters;

    PointPair pair;
    CenterPair centerPair = CenterPair();
    Center center;
    HashCluster cluster;

    // Pointer to points in the right side of the Box.
    auto boxPoint = this->points.begin();

    // Iterator has to initiate on the immediate pointer after the Box's main point.
    for (; (*boxPoint) != this->point; ++boxPoint){}
    ++boxPoint;

    for (; boxPoint != this->points.end(); ++boxPoint) { // For all points in the neighborhood
        // elements inside right half of box
        // if dist(pᵣ, p) <= ε then // calculate pair distance
        if (point.isInRangeQueryWith(*boxPoint, this->range)) {
            pair[0] = point;
            pair[1] = *boxPoint;
            pairs++;
            centerPair = pair.calculateCenters(this->range / 2); // Calculate disk centers

            if (centerPair.valid) {  // There's no problem with NaN
                centers += 2;

                for (int idx = 0; idx < 2; ++idx) { // for each center, compute clusters

                    center = centerPair.get(idx);

                    cluster.clear();
                    cluster.setCenter(center, this->range / 2);

                    for (auto neighbor: this->points) {
                        if (cluster.inRange(neighbor)) {
                            cluster.insert(neighbor);
                        }
                    }

                    if (cluster.size() >= flockSize) {
//                        this->validClusters.push_back(cluster);
                        HashCluster::insertCluster(this->validClusters, cluster);
                    }
                }
            }
        }
    }

    return this->validClusters;
}


vector<PointSet> PlaneBox::getValidClusters(const unsigned int flockSize) {
//    if (this->clusters.size() > 0) return this->clusters;

    PointPair pair;
    CenterPair centerPair = CenterPair();
    Center center;
    PointSet cluster;

    // Pointer to points in the right side of the Box.
    auto boxPoint = this->points.begin();

    // Iterator has to initiate on the immediate pointer after the Box's main point.
    for (; (*boxPoint) != this->point; ++boxPoint){}
    ++boxPoint;

    for (; boxPoint != this->points.end(); ++boxPoint) {
        // elements inside right half of box
        // if dist(pᵣ, p) <= ε then // calculate pair distance
        if (point.isInRangeQueryWith(*boxPoint, this->range)) {
            pair[0] = point;
            pair[1] = *boxPoint;
            pairs++;
            centerPair = pair.calculateCenters(this->range / 2); // Calculate disk centers

            if (centerPair.valid) {  // There's no problem with NaN
                centers += 2;

                for (int idx = 0; idx < 2; ++idx) { // for each center, compute clusters

                    center = centerPair.get(idx);

                    cluster.clear();
                    cluster.setCenter(center, this->range / 2);

                    for (vector<Point>::iterator neighbor = points.begin(); neighbor != points.end(); neighbor++) {
                        if (cluster.inRange(*neighbor)) {
                            cluster.insert(*neighbor);
                        }
                    }

                    if (cluster.size() >= flockSize) {
                        Cluster::insertCluster(this->clusters, cluster);
                    }
                }
            }
        }// End - for j

    }// End - for i

    return this->clusters;
}

//FIXME: the MBR x should be calculated using only the limits points.
void PlaneBox::expand(const Point &point) {

    if (DOUBLE_LES(point.x, nwe.x))
        nwe.x = point.x;
    else if (DOUBLE_GRT(point.x, sea.x))
        sea.x = point.x;

    if (DOUBLE_GRT(point.y, nwe.y))
        nwe.y = point.y;
    else if (DOUBLE_LES(point.y, sea.y))
        sea.y = point.y;
}

bool PlaneBox::intersectsWith(const PlaneBox &box) {
    return (DOUBLE_LEQ(this->nwe.x, box.getSea().x) &&
            DOUBLE_GRE(this->sea.x, box.getNwe().x) &&
            DOUBLE_GRE(this->nwe.y, box.getSea().y) &&
            DOUBLE_LEQ(this->sea.y, box.getNwe().y)
    );
}

void PlaneBox::clear() {
    this->resetMbr();
    this->range = this->centers = this->pairs = 0;
    this->startTime = this->endTime = 0;
    leftPoints = rigthPoints = 0;
    this->membership.clear();
    this->points.clear();
}

void PlaneBox::resetMbr() {
    nwe = point;
    sea = point;
}

unsigned long PlaneBox::getPointCount() {
    return this->points.size();
}

void PlaneBox::addPoint(const Point &point) {
    this->points.push_back(point);
    this->membership.insert(point.oid);

    if (point.x > this->point.x) {
        ++rigthPoints;
    } else {
        ++leftPoints;
    }

    this->expand(point);
}

//--------------- GETTERS AND SETTERS ----------------------------//


unsigned long PlaneBox::getPairs() const {
    return pairs;
}


unsigned long PlaneBox::getCenters() const {
    return centers;
}


const Point &PlaneBox::getSea() const {
    return sea;
}

const Point &PlaneBox::getNwe() const {
    return nwe;
}

//------------- GETTERS AND SETTERS (END) ----------------------//

string PlaneBox::toString() {
    string result = "Box: MBR - {" + to_string(this->nwe.x) + ","
                    + to_string(this->nwe.y) + "; "
                    + to_string(this->sea.x) + "," + to_string(this->sea.y) + "} | pivot: ";
    result += to_string(this->point.oid) + ". " + to_string(this->point.x) + "," + to_string(this->point.y) + "; {";

//    result += " deltaX = " + to_string(this->points.back().x - this->points[0].x);

    for (auto point = points.begin(); point != points.end(); ++point) {
        result += "(" + to_string(point->oid) + ")"; //+ ". " + to_string(point->x) + "," + to_string(point->y) + ")";
        if (point != prev(points.end())) result += ";";
    }

    return result + "}";
}

bool PlaneBox::areCenterPointsCloseEnoughInXWith(const PlaneBox& planeBox) const {
    return (DOUBLE_LEQ(fabs(this->point.x - planeBox.point.x), 2 * this->range));
}

bool PlaneBox::hasPoint(const Point point) {
    for (auto p = points.begin(); p != points.end(); ++p) {
        if (p->oid == point.oid) return true;
    }

    return false;
}

bool PlaneBox::isMember(unsigned int oid) {
    auto itR = this->membership.find(oid);

    return (itR != this->membership.end());
}
