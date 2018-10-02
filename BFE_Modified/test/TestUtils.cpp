//
// Created by denis on 05/07/16.
//

#include "TestUtils.h"

int BasicTests::testOpenStream() {
    return openStream();
}

long BasicTests::testLoadLocations() {
    return loadLocations();
}

long BasicTests::testCountLinesTS0() {

    unsigned int oid;
    double x, y;
    long ts;
    unsigned int countTSLines = 0;

    while(fin >> oid >> x >> y >> ts) {
        if(ts == 0)
            countTSLines++;
        else
            break;
    }

    return countTSLines;
}

void BasicTests::testGoToFinBegin(){
    fin.seekg(0, ios::beg);
}

PointSet BasicTests::testGetMyPoints() const {
    return myPoints;
}
