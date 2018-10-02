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

#include "PlaneSweep.h"
#include <iterator>

class HashPlaneBox;

bool point_x_compare(Point i, Point j) { return (i.x < j.x); }

bool box_oid_compare(PlaneBox i, PlaneBox j) {
    return i.getReferencePoint() < j.getReferencePoint(); // The default less of point is OID
}

bool box_x_compare(PlaneBox i, PlaneBox j) { return point_x_compare(i.getReferencePoint(), j.getReferencePoint()); }

/**
 * Calculate the points that intersect with the box defined by <code>iterator</code>.
 */
template<class TBox>
TBox PlaneSweep::calculateBox(vector<Point>::iterator prevIter,
                              vector<Point>::iterator curIter,
                              vector<Point> &points) {

    TBox box(*curIter, this->distance);
    vector<Point>::iterator i = prevIter;

    for (; i != points.end() && DOUBLE_LEQ(i->x, curIter->x + this->distance); ++i) { //
        // Inside box in x-axis
        if (DOUBLE_GRE(i->y, curIter->y - this->distance) && DOUBLE_LEQ(i->y, curIter->y +
                this->distance)) {
            // Inside box in y-axis
            box.addPoint(*i);
        }
    }

    return box;
}

vector<PlaneBox> PlaneSweep::getLiveBoxes() {

#ifdef PS_DEBUG
    cout << "PlaneSweep::getLiveBoxes()\n";
#endif

    if (this->boxes.empty()) {

        // Sort points by x-axis
        vector<Point> myPoints;
        myPoints.assign(points.begin(), points.end());
        sort(myPoints.begin(), myPoints.end(), point_x_compare);

        vector<Point>::iterator prevIter = myPoints.begin();

        for (vector<Point>::iterator curIter = myPoints.begin(); curIter != myPoints.end(); ++curIter) {
            while (DOUBLE_GRT(fabs(curIter->x - prevIter->x), this->distance))
                ++prevIter;

            PlaneBox box = this->calculateBox<PlaneBox>(prevIter,
                                                        curIter,
                                                        myPoints);// get all points inside the box in x-axis

            // There is enough points in the box.
            if ( box.getPointCount() >= this->flockSize )
                this->boxes.push_back(box); // Add this box to the set of live boxes
        }
    }

#ifdef PS_DEBUG
    cout << "END -- PlaneSweep::getLiveBoxes()\n";
#endif

    return this->boxes;
}

vector<HashCluster> PlaneSweep::getValidHashClusters() {
    clock_t timeBoxes, p_clusters, p_timeClusters;
    timeBoxes = clock();
    vector<PlaneBox> boxes = getLiveBoxes();
    this->timeBoxes += (clock() - timeBoxes) / CLOCK_DIV;

#ifdef PS_STEPS_DBG
    cout << endl << "Time search boxes (ms): " << (clock() - timeBoxes) / (CLOCKS_PER_SEC / 1000) << endl;
#endif

    vector<HashCluster> finalClusters;
    vector<HashCluster> clusters;

#ifdef PS_DEBUG
    cout << "PlaneSweep::getValidClusters()" << endl;
    int iter = 0;
#endif

    for (vector<PlaneBox>::iterator box = boxes.begin(); box != boxes.end(); ++box) {
        p_clusters = clock();

        clusters = box->getValidHashClusters(this->flockSize);

        this->timeBoxClustering += (clock() - p_clusters) / CLOCK_DIV;
        this->centers += box->getCenters();
        this->pairs += box->getPairs();

        // if |c ∩ P| >= µ then // check the number of entries in disk
        if (clusters.empty()) {
            continue;
        }

        this->clusterCount += clusters.size();

        vector<HashCluster>::iterator cluster = clusters.begin();

        p_timeClusters = clock();

        for (vector<PlaneBox>::iterator nextBox = box + 1; nextBox != boxes.end() &&
                box->areCenterPointsCloseEnoughInXWith(*nextBox); ++nextBox) {
            if (box->intersectsWith(*nextBox)) {
                for (; cluster != clusters.end(); ++cluster) {
                    HashCluster::insertCluster(finalClusters, *cluster);
                }

                break;
            }
        }

        // not the end of the clusters in current box
        if (cluster < clusters.end()) {
            // and there's no more intersection
            for (; cluster != clusters.end(); ++cluster) {
                finalClusters.push_back(*cluster);
            }
        }

        this->timeSweepClustering += ((clock() - p_timeClusters) / CLOCK_DIV);

#ifdef PS_DEBUG
        if ( (++iter % 200) == 0) {
            cout << "box " << iter << " of " << boxes.size() << endl;
        }
        if(iter == boxes.size() - 1) break;
#endif

    } // for boxes


#ifdef PS_DEBUG
    cout << "End getValidClusters" << endl;
#endif
    return finalClusters;
}

vector<PointSet> PlaneSweep::getValidClusters() {
    clock_t timeBoxes, p_clusters, p_timeClusters;
    timeBoxes = clock();
    vector<PlaneBox> boxes = getLiveBoxes();
    this->timeBoxes += (clock() - timeBoxes) / CLOCK_DIV;

#ifdef PS_STEPS_DBG
    cout << endl << "Time search boxes: " << (clock() - timeBoxes) / CLOCK_DIV << endl;
#endif

    vector<PointSet> finalClusters;
    vector<PointSet> clusters;

#ifdef PS_DEBUG
    cout << "PlaneSweep::getValidClusters()" << endl;
    int iter = 0;
#endif

    for (vector<PlaneBox>::iterator box = boxes.begin(); box != boxes.end(); ++box) {
        p_clusters = clock();

        clusters = box->getValidClusters(this->flockSize);

        this->timeBoxClustering += (clock() - p_clusters) / CLOCK_DIV;
        this->centers += box->getCenters();
        this->pairs += box->getPairs();

        // if |c ∩ P| >= µ then // check the number of entries in disk
        if (clusters.empty()) {
            continue;
        }

        this->clusterCount += clusters.size();

        vector<PointSet>::iterator cluster = clusters.begin();

        p_timeClusters = clock();

        for (vector<PlaneBox>::iterator nextBox = box + 1; nextBox != boxes.end() &&
                box->areCenterPointsCloseEnoughInXWith(*nextBox); ++nextBox) {
            if (box->intersectsWith(*nextBox)) {
                for (; cluster != clusters.end(); ++cluster) {
                    Cluster::insertCluster(finalClusters, *cluster);
                }

                break;
            }
        }

        // not the end of the clusters in current box
        if (cluster < clusters.end()) {
            // and there's no more intersection
            for (; cluster != clusters.end(); ++cluster) {
                finalClusters.push_back(*cluster);
            }
        }


        this->timeSweepClustering += ((clock() - p_timeClusters) / CLOCK_DIV);

#ifdef PS_DEBUG
        if ((iter % 100) == 0) {
            cout << "box " << ++iter << " of " << boxes.size() << endl;
        }
        if(iter == boxes.size() - 1) break;
#endif

    } // for boxes


#ifdef PS_DEBUG
    cout << "End getValidClusters" << endl;
#endif
    return finalClusters;
}

unsigned long PlaneSweep::getCenters() const {
    return centers;
}

double PlaneSweep::getTimeSweepClustering() const {
    return timeSweepClustering;
}

double PlaneSweep::getTimeBoxes() const {
    return timeBoxes;
}

unsigned long PlaneSweep::getPairs() const {
    return pairs;
}

unsigned long PlaneSweep::getClusterCount() const {
    return clusterCount;
}
