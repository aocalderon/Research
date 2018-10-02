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

#ifndef __FLOCKBASE__
#define __FLOCKBASE__

#include "Cluster.h"
#include "FlockAnswer.h"
#include "GridIndex.h"
#include "PlaneSweep.h"

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <cstring>
#include <vector>
#include <map>
#include <set>

using namespace flockcommon;
using namespace std;

//------------------------------------------------------------------------------
class FlockBase {
public:
    FlockBase(unsigned int nro, unsigned int len,
              double dist, string filename) {
        this->nroFlocks = nro;
        this->lengthFlocks = len;
        this->distanceFlocks = dist * INCFACTOR;
        this->inFileName = filename;

        radius = this->distanceFlocks / 2.0;
        myFlockAnswers.setNroFlocks(this->nroFlocks);
        myFlockAnswers.setLength(this->lengthFlocks);
    }

    ~FlockBase() {
        if (fin.is_open())
            fin.close();

        delete mySweep;
        delete myGridIndex;
    }

    bool createLogger(std::string);

    unsigned int getTotalAnswers() const;

    unsigned int getTotalCenters() const;

    unsigned int getTotalClusters() const;

    unsigned int getTotalPairs() const;

protected:

    int openStream();

    int closeStream();

    long getLocation(Point *point);

    long loadLocations();

    bool logMessage(std::string);

    bool loggerIsGood();

    FlockAnswer myFlockAnswers;
    GridIndex *myGridIndex = nullptr;
    ifstream fin;
    ofstream loggingStream;
    PlaneSweep *mySweep = nullptr;
    Point currentLocation;
    PointSet myPoints;

    double distanceFlocks = 0;
    double maxX = MAX_GRID;
    double maxY = MAX_GRID;
    double minX = MIN_GRID;
    double minY = MIN_GRID;
    double radius = 0;
    long currentTimestamp = 0;
    long double timeJoin = 0;
    long double timeSearch = 0;
    long double timeSetTest = 0;
    string inFileName;
    unsigned int lengthFlocks = 0;
    unsigned int maxOid = 0;
    unsigned int nroFlocks = 0;
    unsigned int totalAnswers = 0;
    unsigned int totalCenters = 0;
    unsigned int totalClusters;
    unsigned int totalPairs = 0;
    vector<Cluster> myFlocks;

};//end class

#endif //__FLOCKBASE__
