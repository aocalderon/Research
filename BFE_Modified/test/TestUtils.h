#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include "gtest/gtest.h"
#include "Point.h"
#include <iostream>
#include <sstream>
#include <istream>
#include <fstream>

using namespace std;
using namespace flockcommon;

#define DEF_NUM 5
#define DEF_DELTA 8
#define DEF_DIST 0.7

#define MIN_NUM 4
#define MAX_NUM 20
#define MIN_DELTA 4
#define MAX_DELTA 20
#define DATA_FILE "../flocks/data/buses_by_time.txt"
#define INC_NUM 4
#define INC_DELTA 4

class TestUtils {

    public:
        static set<Point> readTestFile(const char * fileName = "test/data/flock.txt") {

        char *workingDir = getenv("WKD");
        string file(fileName);

        if (workingDir != NULL) {
            string tmp(workingDir);

            if (tmp.back() != '/') tmp += '/';

            tmp += file;
            file = tmp;
        }

        ifstream stream(file.c_str());
        double x, y;
        unsigned int oid, ts;

        set<Point, less<Point>> points;

        while (stream >> oid >> x >> y >> ts) {
            Point p(x, y, oid);
            points.insert(p);
        }

        return points;
    }
};

#endif

#ifndef BASIC_TESTS
#define BASIC_TESTS

#include "BasicFlock.h"
class BasicTests : public BasicFlock {
public:
    BasicTests(unsigned int nroFlocks, unsigned int lengthFlocks,
               double distanceFlocks, string inFileName) :
            BasicFlock(nroFlocks, lengthFlocks, distanceFlocks, inFileName) {
    }

    explicit BasicTests(string inFileName="../flocks/data/tests.txt") :
            BasicFlock(3, 3, 3, inFileName) {}

    /*
     * Public FlockBase::openStream().
     * */
    int testOpenStream();

    /*
     * public FlockBase::loadLocations().
     * */
    long testLoadLocations();

    /*
     * Count the number of lines with timestamp zero in the dataset.
     * */
    long testCountLinesTS0();

    /*
     * Returns to the fin file beginnin.
     * */
    void testGoToFinBegin();

    /*
     * FlockBase::myPoints getter.
     * */
    PointSet testGetMyPoints() const;
};

#endif
