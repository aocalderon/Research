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

#ifndef FLOCKS_HASHPLANEBOX_H
#define FLOCKS_HASHPLANEBOX_H

#include "PlaneBox.h"
#include "Hashable.hpp"


class HashPlaneBox : public PlaneBox, public Hashable {

public:

    HashPlaneBox(const Point &point, double range);


    void addPoint(const Point &point) override;

    virtual void insert(const Point point);

protected:
    void clear() override;
};


#endif //FLOCKS_HASHPLANEBOX_H
