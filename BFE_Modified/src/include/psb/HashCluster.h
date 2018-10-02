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

#ifndef FLOCKS_HASHCLUSTER_H
#define FLOCKS_HASHCLUSTER_H
#define HC_SET_EQ 0
#define HC_SUBSET 1
#define HC_UNCERT 2


#include <bitset>
#include <set>
#include <vector>

#include "Point.h"
#include "PointSet.h"
#include "Hash.h"
#include "Hashable.hpp"


using namespace std;
using namespace flockcommon;


class HashCluster : public PointSet, public Hashable {

public:
    HashCluster();

    HashCluster(const HashCluster &other);

    HashCluster(const set<Point> &myPoints, const vector<Hash *> &functions);

    HashCluster(const vector<Hash*> &functions);

    void operator=(const HashCluster &other);

    bool operator==(const HashCluster &other);

    void reInsertPoints(set<Point> points);

    virtual void insert(const Point p) override;

    int isSubset(const HashCluster &other);

    void clear();

    static void insertCluster(vector<HashCluster> &collection, HashCluster cluster);


private:
    friend std::ostream& operator<<(std::ostream&, HashCluster&);
};




#endif //PSBHASHES_HASHCLUSTER_H

