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

#include "PointSet.h"

bool point_oid_cmp(const flockcommon::Point &a, const flockcommon::Point &b) {
    return a.oid < b.oid;
}

namespace flockcommon {

    PointSet PointSet::operator=(PointSet pSet) {
        if(this != &pSet) {
            swap(this->points, pSet.points);
            this->pruning = pSet.pruning;
            this->centerX = pSet.centerX;
            this->centerY = pSet.centerY;
            this->radius = pSet.radius;
            this->time = pSet.time;
        }

        return *this;
    }

    set<::flockcommon::Point>::const_iterator PointSet::cbegin() const {
        return points.cbegin();
    }

    set<::flockcommon::Point>::const_iterator PointSet::cend() const {
        return points.cend();
    }


    void PointSet::invalidateCenter() {
        pruning = false;
    }

    void PointSet::setCenter(Center c, double r) {
        centerX = c.x;
        centerY = c.y;
        radius = r;
        pruning = true;
    }

    void PointSet::clear() {
        points.clear();
        centerX = 0;
        centerY = 0;
        radius = 0;
        pruning = false;
    }

    bool PointSet::usePruning() {
        return pruning;
    }

    void PointSet::insert(const Point p) {
        points.insert(p);
    }

    void PointSet::insert(const my_iter it1, const my_iter it2) {
        points.insert(it1, it2);
    }

    unsigned int PointSet::size() {
        return points.size();
    }

    bool PointSet::empty() const {
        return points.empty();
    }

    string PointSet::toString() {
        string str;
        str += "Flock: size " + to_string(points.size()) + ", {";

        for (auto i = points.begin(); i != points.end(); ++i) {
            str += to_string(i->oid);

            if (i != prev(points.end())) str += ", ";
        }
        str += "}";

        return str;
    }

    /**
     * Check if a point set is contained in other
     */
    bool PointSet::contains(const PointSet other) {
        return includes(this->points.begin(), this->points.end(),
                        other.cbegin(), other.cend(), point_oid_cmp);
    }

    bool PointSet::hasPoint(Point point) {
        my_iter i = this->points.find(point);
        return i != points.cend() && i != points.end();
    }

    bool PointSet::inRange(Point point, DistanceMetric metric) {
        Point p(this->centerX, this->centerY);
        double distance = p.distance(point, metric); // for debug reasons

        return DOUBLE_LEQ(distance, radius);
    }

    string PointSet::getOids(char delimiter) {
        string out;

        for (auto point = points.begin(); point != points.end(); ++point) {
            if (point != points.begin()) out += delimiter;
            out += to_string(point->oid);
        }
        return out;
    }

}
