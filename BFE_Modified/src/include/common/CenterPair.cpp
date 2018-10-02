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

#include "CenterPair.h"

namespace flockcommon {

    Center &CenterPair::operator[](int idx) {
        return this->centers[idx];
    }

    Center CenterPair::get(int idx) {
        return this->centers[idx];
    }

    bool CenterPair::operator<(CenterPair c) const {
        if (DOUBLE_LES(this->centers[0].x, c.get(0).x)) return true;
        if (DOUBLE_NEQ(this->centers[0].x, c.get(0).x)) return false;
        if (DOUBLE_LES(this->centers[0].y, c.get(0).y)) return true;
        if (DOUBLE_NEQ(this->centers[0].y, c.get(0).y)) return false;
        if (DOUBLE_LES(this->centers[1].x, c.get(1).x)) return true;
        if (DOUBLE_NEQ(this->centers[1].x, c.get(1).x)) return false;
        if (DOUBLE_LES(this->centers[1].y, c.get(1).y)) return true;

        return false;
    }

    void CenterPair::operator=(CenterPair c) {
        this->valid = c.valid;
        this->centers[0].x = c.get(0).x;
        this->centers[0].y = c.get(0).y;
        this->centers[1].x = c.get(1).x;
        this->centers[1].y = c.get(1).y;
    }
}
