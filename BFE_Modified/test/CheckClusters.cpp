/**
 * This file includes an applicaiton to check whether clusters are being found in a file.
 * The application first reads all the points from the file passed as parameter and then calculates the
 * clusters found between this points with the given parameters.
 */

#include "Point.h"
#include <iostream>
#include <sstream>
#include <set>
#include <fstream>
#include <psb/PlaneSweep.h>
#include "tclap/CmdLine.h"

using namespace std;
using namespace flockcommon;
using namespace TCLAP;

set<Point> readFile(const char *fileName = "test/data/flock.txt") {
    ifstream stream(fileName);
    double x, y;
    unsigned int oid, trash;

    set<Point, less<Point>> points;

    while (stream.good()) {
        stream >> oid;
        stream >> x;
        stream >> y;
        stream >> trash;

        Point p(x, y, oid);
        points.insert(p);
    }

    return points;
}


void checkForClusters(const string &fileName, const int size, const double &distance,
                              const bool showPoints, const bool showBoxes) {

    set<Point> points = readFile(fileName.c_str());

    PlaneSweep ps = PlaneSweep(distance, size, points);

    vector<PointSet> clusters = ps.getValidClusters();

    // #FIXME Find a way to test this again.
//    cout << "Found "  << ps.getLiveBoxes().size() << " boxes!\n";

    cout << "Found " << clusters.size() << " clusters!\n";

    for (auto cluster = clusters.begin(); cluster != clusters.end(); ++cluster) {
        cout << cluster->toString() << "; cx: " << cluster->centerX << ", cy:" << cluster->centerY << ";\n";
    }

    cout << "#### END CLUSTERS" << endl;

 /*
    // #FIXME Find a way to test this again.
    if (showBoxes) {
        cout << "## BEGIN BOXES ##\n";

        for (auto box = ps.getLiveBoxes().begin(); box != ps.getLiveBoxes().end(); ++box) {
            cout << box->toString() << "\n";
        }

        cout << "## END BOXES ##" << endl;
    }
*/
//    HashPlaneBox box(*points.begin(), distance);
//
//    for (auto point = points.begin(); point != points.end(); ++point) {
//        box.addPoint(*point);
//    }
//
//
//    vector<HashCluster> clusters = box.getValidHashClusters(size);
//
//    cout << "Found " << clusters.size() << " clusters for params: " << size << ", " << distance << endl << endl;
//    for (auto cluster = clusters.begin(); cluster != clusters.end(); ++cluster) {
//        cout << cluster->toString() << endl;
//    }
//
    if (showPoints) {
        cout << endl << "#### POINTS ####" << endl;

        for (auto point = points.begin(); point != points.end(); ++point) {
            cout << point->serialize();

            if (point != points.end()) cout << endl;
        }

        cout << "#### POINTS ####" << endl;
    }

}

void checkForClustersGrid(string fileName, int size, double &distance) {

}


int main(int argc, char *argv[]) {

    // Wrap everything in a try block.  Do this every time,
    // because exceptions will be thrown for problems.
    try {

        // Define the command line object, and insert a message
        // that describes the program. The "Command description message"
        // is printed last in the help text. The second argument is the
        // delimiter (usually space) and the last one is the version number.
        // The CmdLine object parses the argv array based on the Arg objects
        // that it contains.
        CmdLine cmd("Check for the existence of clusters using a file", ' ', "1.0b");

        // Define a value argument and add it to the command line.
        // A value arg defines a flag and a type of value that it expects,
        // such as "-n Bishop".
        ValueArg<string> nameArg("f", "file", "file name (relative or absolute)", true, "file.txt", "string");
        ValueArg<int> sizeArg("s", "size", "flock size (mu)", true, 4, "integer value");
        ValueArg<double> distArg("d", "distance", "flock distance (eps)", true, 1.1, "decimal value");
        SwitchArg showPoints("v", "show-points", "show all points from input file", false);
        SwitchArg showBoxes("b", "show-boxes", "show all detected boxes", false);


        // Add the argument nameArg to the CmdLine object. The CmdLine object
        // uses this Arg to parse the command line.
        cmd.add(nameArg);
        cmd.add(sizeArg);
        cmd.add(distArg);
        cmd.add(showPoints);
        cmd.add(showBoxes);

        // Parse the argv array.
        cmd.parse(argc, (const char *const *) argv);

        // Get the value parsed by each arg.
        std::string fileName = nameArg.getValue();

        // Do what you intend.
        checkForClusters(fileName, sizeArg.getValue(),
                         distArg.getValue(), showPoints.getValue(),
                         showBoxes.getValue());


    } catch (TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
    }

}
