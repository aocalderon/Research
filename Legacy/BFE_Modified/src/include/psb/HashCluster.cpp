/*
Flock Evaluation Algorithms

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

#include <common/Cluster.h>
#include "HashCluster.h"

HashCluster::HashCluster() {
    createHashes();
}

HashCluster::HashCluster(const vector<Hash*> &functions) {
    this->functions = functions;
}

HashCluster::HashCluster(const set<Point> &myPoints, const vector<Hash *> &functions) {
    this->functions = functions;
    reInsertPoints(myPoints);
}

HashCluster::HashCluster(const HashCluster &other) {
    this->points = other.points;
    this->signature = other.signature;
    this->functions = other.functions;
}

bool HashCluster::operator==(const HashCluster &other) {
    return this->getSignature() == other.getSignature();
}

std::ostream &operator<<(std::ostream &out, HashCluster &cluster) {
    out << "HashCluster[(";
    for (auto point = cluster.points.begin(); point != cluster.points.end(); ++point) {
        out << to_string(point->oid);
        if (point != prev(cluster.points.end())) out << ", ";
    }
    out << "), " << cluster.signature << "]";

    return out;
}

void HashCluster::operator=(const HashCluster &other) {
    this->points = other.points;
    this->signature = other.signature;
    this->functions = other.functions;
}


void HashCluster::insert(const Point point) {
    this->hashPoint(point);
    points.insert(point);
}


void HashCluster::reInsertPoints(set<Point> points) {
    for (auto point = points.begin(); point != points.end(); ++point) {
        insert(*point);
    }
}

/**
 * Check if this Cluster is a subset of other cluster.
 * This method returns:
 *  * 0 case the two clusters are equal.
 *  * 1 case this is a subset of other
 *  * 2 otherwise
 *
 */
int HashCluster::isSubset(const HashCluster &other) {
    bitset<SIGN_SIZE> result = other.getSignature() & this->getSignature();

    if (other.getSignature() == this->getSignature()) {
        return 0;
    } else if (result == this->getSignature()) {
        return 1;
    } else {
        return 2;
    }
}

void HashCluster::insertCluster(vector<HashCluster> &collection, HashCluster cluster) {
    if (collection.empty()) {
        collection.push_back(cluster);
        return;
    }

    for (auto oldClt = collection.begin(); oldClt != collection.end(); ) {
        int result = cluster.isSubset(*oldClt);
        #ifdef FHASH_DEBUG
            cout << "\nIterator : " << oldClt->getSignature() << "(" << oldClt->getOids() << ")" << "\n";
            cout << "CLuster  : " << cluster.getSignature() << "(" << cluster.getOids() << ")" << endl;
        #endif
        if (result == HC_SUBSET || result == HC_SET_EQ) { // Is subset or equal, do not insert
            // Remove false positives TODO: check if is necessary
            PointSet res = Cluster::setIntersection(*oldClt, cluster);
            if (res.points.size() == cluster.size()) {
                return;
            } else {
                oldClt++;
            }
        } else if (oldClt->isSubset(cluster) != HC_SUBSET) { // FIXME: Could be a pitfall, check if there's not case missing
            oldClt++;
        } else {
            collection.erase(oldClt);
        }
    }

    collection.insert(collection.end(), cluster);
}

void HashCluster::clear() {
    PointSet::clear();
    this->signature.reset();
}
