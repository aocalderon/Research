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

#ifndef FLOCKS_DEFINITIONS_HPP
#define FLOCKS_DEFINITIONS_HPP

#include <cmath>


#define MY_EPSILON 0.001
#define DOUBLE_EQ(x, y)  (fabs(double(x - y)) < MY_EPSILON)
#define DOUBLE_NEQ(x, y) (!DOUBLE_EQ(x, y))
#define DOUBLE_LES(x, y) (x < y)
#define DOUBLE_LEQ(x, y) ((x < y) || DOUBLE_EQ(x,y))
#define DOUBLE_GRT(x, y) (x >  y)
#define DOUBLE_GRE(x, y) ((x > y) || DOUBLE_EQ(x,y))


//#define __DEBUGSTEPS__
//#define __DEBUGPRINT__
#define __PRINTANSWER__
//#define __PRINTCLUSTERS__
//#define __MAXTIME__
//#define MAXTIME 200
//#define __DEBUGMODE__
#define INCFACTOR 1
//#define __DEBUGCENTER__
#define __RADIUSPRUNING__

// (double)CLOCKS_PER_SEC
#define CLOCK_DIV 1000.0
//#define __PRINTTIMESTATUS
#define MIN_GRID 900
#define MAX_GRID 1000

namespace flockcommon {


    enum FLOCKQUERYMETHOD {
        BFE_METHOD,          // Basic Flock Evaluation
        BFE_PSB_IVIDX_METHOD // Plane Sweep Inv Index + Bin Sig (PSI)
    };
}

#endif //FLOCKS_DEFINITIONS_HPP
