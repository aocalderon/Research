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

#include "PointPair.h"

namespace flockcommon {

    PointPair PointPair::operator=(PointPair p) {
        if (this != &p) {
            points[0].x = p.get(0).x;
            points[0].y = p.get(0).y;
            points[0].oid = p.get(0).oid;
            points[1].x = p.get(1).x;
            points[1].y = p.get(1).y;
            points[1].oid = p.get(1).oid;
        }

        return *this;
    }

    bool PointPair::operator<(PointPair p) const {
        if (points[0].oid < p.get(0).oid) return true;
        if (points[0].oid != p.get(0).oid) return false;
        if (points[1].oid < p.get(1).oid) return true;
        return false;
    }

    Point PointPair::get(int idx) {
        return points[idx];
    }//end

    Point &PointPair::operator[](int idx) {
        return points[idx];
    }//end

    CenterPair PointPair::calculateCenters(double radius) {
        /*  LINES COMMENTED
        double A, B, C, a1, a2, b1, b2, delta;
        double a1squared, a2squared, b1squared, b2squared, twob1b2, twob2minusb1, tmp; // intermediate results
        */
        
        CenterPair centers;
        centers.valid = false; // default value
        errno = 0; // for errors
        // test to see if they are TOO close!
        if (DOUBLE_EQ(points[0].x, points[1].x) && DOUBLE_EQ(points[0].y, points[1].y))
            return centers;
            
        /* LINES ADDED START */
        double x1 = points[0].x;
        double y1 = points[0].y;
        double x2 = points[1].x;
        double y2 = points[1].y;

        double  X = x1 - x2;
        double  Y = y1 - y2;

        double D2 = X * X + Y * Y;
        double R2 = radius * radius;

        if(D2 != 0.0){
            double root = sqrt(fabs(4.0 * (R2 / D2) - 1.0));
            // calculating x
            double h1 = ((X + Y * root) / 2.0) + x2;
            centers[0].x = h1;
            double h2 = ((X - Y * root) / 2.0) + x2;
            centers[1].x = h2;
            // calculating y
            double k1 = ((Y - X * root) / 2.0) + y2;
            centers[0].y = k1;
            double k2 = ((Y + X * root) / 2.0) + y2;
            centers[1].y = k2;
        }
        /* LINES ADDED END */

        /*  LINES COMMENTED
        a1 = points[0].x;
        b1 = points[0].y;
        a2 = points[1].x;
        b2 = points[1].y;
        a1squared = a1 * a1;
        a2squared = a2 * a2;
        b1squared = b1 * b1;
        b2squared = b2 * b2;
        twob1b2 = 2.0 * b1 * b2;
        twob2minusb1 = 2.0 * (b2 - b1);
        tmp = a2squared - a1squared - b1squared + b2squared;
        A = (a1 - a2) / (b2 - b1);
        A = A * A + 1.0; // 1 + ((a1-a2)/(b2-b1))*((a1-a2)/(b2-b1))
        B = 2.0 * ((a1 - a2) / (b2 - b1) * ((tmp - twob1b2 + 2.0 * b1squared) / twob2minusb1) - a1);
        C = a1squared - radius * radius +
            ((tmp - twob1b2 + 2.0 * b1squared) / twob2minusb1) *
            ((tmp - twob1b2 + 2.0 * b1squared) / twob2minusb1);
        delta = B * B - 4.0 * A * C;
        // calculating x
        if (delta < 0) return centers;
        if (DOUBLE_EQ(A, 0)) return centers;
        if (DOUBLE_EQ(b2, b1)) return centers;

        centers[0].x = (-B + sqrt(delta)) / (2.0 * A);
        centers[1].x = (-B - sqrt(delta)) / (2.0 * A);
        // calculating y
        centers[0].y = (2.0 * a1 * centers[0].x - 2.0 * a2 * centers[0].x + tmp) / twob2minusb1;
        centers[1].y = (2.0 * a1 * centers[1].x - 2.0 * a2 * centers[1].x + tmp) / twob2minusb1;
        */
        
        if (errno != EDOM && errno != ERANGE)
            centers.valid = true; // everything went okay

        return centers;
    }//end

}
