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

#include "Center.h"

namespace flockcommon {
    bool Center::operator<(Center p) const { // ordered by (x, y)
        if (DOUBLE_LES(this->x, p.x)) return true;
        if (DOUBLE_NEQ(this->x, p.x)) return false;
        return DOUBLE_LES(this->y, p.y);
    }

    void Center::operator=(Center p) {
        this->x = p.x;
        this->y = p.y;
    }
}
