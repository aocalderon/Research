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

#include <cstring>
#include <iostream>
#include "Cluster.h"

using namespace std;

namespace flockcommon {

//------------------------------------------------------------------------------
    unsigned int Cluster::countIntersection(PointSet pnts1, PointSet pnts2) {
        PointSet::my_iter it1, it2;
        PointSet::my_iter it_last1, it_last2;
        double tmp, dist;
        unsigned int count = 0;

#ifdef __RADIUSPRUNING__
        if (pnts1.usePruning() && pnts2.usePruning()) {
            ////////////////////////////////////////////////
            tmp = pnts1.centerX - pnts2.centerX;
            dist = tmp * tmp;
            tmp = pnts1.centerY - pnts2.centerY;
            dist += tmp * tmp;
            dist = sqrt(dist);

            if (DOUBLE_GRT(dist, 2.0 * pnts1.radius)) return 0;
            ////////////////////////////////////////////////
        }//end if
#endif

        it1 = pnts1.begin();
        it2 = pnts2.begin();
        // check for the ranges.
        it_last1 = pnts1.end();
        it_last2 = pnts2.end();
        it_last1--;
        it_last2--;

        if (*it1 < *it2 && *it_last1 < *it2) return 0;
        else if (*it2 < *it1 && *it_last2 < *it1) return 0;

        // computer the intersection.
        while (it1 != pnts1.end() && it2 != pnts2.end()) {
            if (*it1 < *it2) {
                ++it1;
            } else if (*it2 < *it1) {
                ++it2;
            } else {
                count++;
                it1++;
                it2++;
            }
        }//end while

        return count;
    }//end

//------------------------------------------------------------------------------
    PointSet Cluster::setIntersection(PointSet &pnts1, PointSet &pnts2) {
        PointSet::my_iter it1, it2;
        PointSet::my_iter it_last1, it_last2;
        PointSet inter;

        inter.clear();  // first make sure inter set is clear.
#ifdef __RADIUSPRUNING__
        ////////////////////////////////////////////////
        if (pnts1.usePruning() && pnts2.usePruning()) {
            double tmp, dist;
            tmp = pnts1.centerX - pnts2.centerX;
            dist = tmp * tmp;
            tmp = pnts1.centerY - pnts2.centerY;
            dist += tmp * tmp;
            dist = sqrt(dist);

            if (DOUBLE_GRT(dist, 2.0 * pnts1.radius)) return inter;
        }//end if
        //////////////////////////////////////////////// */
#endif

        it1 = pnts1.begin();
        it2 = pnts2.begin();
        // check for the ranges.
        it_last1 = pnts1.end();
        it_last2 = pnts2.end();
        it_last1--;
        it_last2--;

        if (*it1 < *it2 && *it_last1 < *it2) return inter;
        else if (*it2 < *it1 && *it_last2 < *it1) return inter;

        // compute the intersection.
        while (it1 != pnts1.end() && it2 != pnts2.end()) {
            if (*it1 < *it2) ++it1;
            else if (*it2 < *it1) ++it2;
            else {
                inter.insert(*it1);
                it1++;
                it2++;
            }
        }//end while

        return inter;  // result the intersection of two sets
    }//end

//------------------------------------------------------------------------------
    void Cluster::insertCluster(vector<PointSet> &clusters, PointSet &pnts) {

        if (clusters.size() == 0) { // it's the first time
            clusters.insert(clusters.end(), pnts); // no need to test anything.
            return; // stop it.
        }

        vector<PointSet>::iterator itClts;
        unsigned int count;

        // look for supersets of cluster in clusters
        for (itClts = clusters.begin(); itClts != clusters.end();) {
            count = Cluster::countIntersection(*itClts, pnts);

            // let's try it!
            if (pnts.size() == count) return; // cluster subset of pnts. don't insert cluster.

            if ((*itClts).size() != count) {
                ++itClts; // pnts is not a subset of cluster. keep it!
            } else {
                clusters.erase(itClts);  // delete it.
            }//end if
        }//end for

        // we can safe insert pnts.
        clusters.insert(clusters.end(), pnts);
    }//end

//------------------------------------------------------------------------------
    void Cluster::insertNewFlock(vector<Cluster> &clusters, Cluster newClt) {

        if (clusters.size() == 0) { // it's the first time
            clusters.insert(clusters.end(), newClt); // no need to test anything.
            return; // stop it.
        }

        vector<Cluster>::iterator itClts;
        unsigned int count;

        // look for supersets of newClt in clusters
        for (itClts = clusters.begin(); itClts != clusters.end();) {
            count = countIntersection((*itClts).points, newClt.points);

            // first try for the case: clt is a subset of itClts, and (*itClts).t <= newClt.t
            if (newClt.points.size() == count) { // newClt subset of (*itClts)
                if ((*itClts).startTime <= newClt.startTime) { // (*itClts) is longer in time.
                    return; // no need to insert it!
                } else if ((*itClts).points.size() >
                           count) { // itClts and newClt do not have the same number of points.
                    ++itClts; // we have to keep (*itClts).
                } else {
                    clusters.erase(itClts); // remove it
                }//end if
            } else if ((*itClts).points.size() == count) {
                if ((*itClts).startTime < newClt.startTime) { // keep the old one.
                    ++itClts; // (*itClts) is not a subset of clt.
                } else {
                    clusters.erase(itClts); // remove it
                }//end if
            } else { // Not enough common elements among clusters
                ++itClts;   // keep the old one.
            }//end if
        }//end for

        // we can safe insert clt.
        clusters.insert(clusters.end(), newClt);
    }//end - insertNewFlock

}

