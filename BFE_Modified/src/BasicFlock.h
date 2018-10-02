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

#ifndef __BASICFLOCK__
#define __BASICFLOCK__

#include "FlockBase.h"

using namespace std;
using namespace flockcommon;

//------------------------------------------------------------------------------
class BasicFlock : public FlockBase {
public:
    BasicFlock(unsigned int nroFlocks, unsigned int lengthFlocks,
               double distanceFlocks, string inFileName) :
            FlockBase(nroFlocks, lengthFlocks, distanceFlocks, inFileName) {
    }

    unsigned int start(enum FLOCKQUERYMETHOD type);

private:

    void queryJoin(unsigned int , FLOCKQUERYMETHOD , unsigned long &, unsigned long &, unsigned long &);

};//end class

#endif //__BASICFLOCK__
