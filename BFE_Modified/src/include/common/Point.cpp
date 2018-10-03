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

#include <stdexcept>
#include "Point.h"

namespace flockcommon {

    double Point::distance(const Point &p, const DistanceMetric &metric) const {
        switch (metric) {
            case DistanceMetric::L2 : {
                double tmp = this->x - p.x;
                double dist = tmp * tmp;
                tmp = this->y - p.y;
                dist += tmp * tmp;

                return sqrt(dist);
            };
            case DistanceMetric::L1: {
                return fabs(this->x - p.x) + fabs(this->y - p.y);
            };
            case DistanceMetric::CAN: {
                double sum = 0.0;
                sum += fabs(this->x - p.x) / (fabs(this->x) + fabs(p.x));

                return sum + fabs(this->y - p.y) / (fabs(this->y) + fabs(p.y));
            };
            case DistanceMetric::INF: {
                return std::max(fabs(this->x - p.x), fabs(this->y - p.y));
            };
            default:
                throw std::invalid_argument("Unknown DistanceMetric.");
        }
    }

    string Point::serialize() const {
        return "Point { oid: " + to_string(oid)
               + " x: " + to_string(x)
               + " y: " + to_string(y) + "}";
    }

    bool Point::operator<(Point p) const {
        return (this->oid < p.oid); // ordered by Oid
    }

    bool Point::isInRangeQueryWith(const Point &point, double &range) const {
        return DOUBLE_LEQ(this->distance(point), range);
    }

    bool Point::operator==(const Point &p) const {
        return this->x == p.x && this->y == p.y;
    }

    bool Point::operator!=(const Point &p) const {
        return !(*this == p);
    }

    void Point::operator=(const Point &p) {
        this->x = p.x;
        this->y = p.y;
        this->oid = p.oid;
    }

    /*
    For each point pr ∈ gx,y (this), a range query with radius ε (distThreshold) is performed
    over all 9 grids [gx−1,y−1...gx+1,y+1] to find points that can be “paired”with pr, that is d
    (pr, ps) ≤ ε holds. The result of such range search with more or equal points than µ (|H| ≥
    µ) is stored in the list H that is used to check for each disk computed.
    */
    set<Point, less<Point>>* Point::getNeighborhood(const set<Point, less<Point>> &pnts, const
    double &distThreshold) const {
        set<Point, less<Point>> *neighborhood = new set<Point, less<Point>>();

        for (set<Point, less<Point> >::iterator it = pnts.cbegin(); it != pnts.cend(); ++it) {
            if(DOUBLE_LEQ(this->distance(*it), distThreshold))
                neighborhood->insert(*it);
        }

        return neighborhood;
    }
}
