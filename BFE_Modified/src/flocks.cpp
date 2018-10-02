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

#include <iostream>
#include "tclap/CmdLine.h"
#include "FlockRunner.h"

using namespace std;
using namespace TCLAP;
using namespace flockcommon;


int main(int argc, char *argv[]) {
    // Wrap everything in a try block.  Do this every time,
    // because exceptions will be thrown for problems.

    try {
        FlockRunner runner;

        ValuesConstraint<string> allowedValues(runner.getAllowedMethods());
        // Define the command line object, and insert a message
        // that describes the program. The "Command description message"
        // is printed last in the help text. The second argument is the
        // delimiter (usually space) and the last one is the version number.
        // The CmdLine object parses the argv array based on the Arg objects
        // that it contains.
        CmdLine cmd("Run the a specified algorithm to find flocks in a trajectory DB.", ' ', "2.0b");

        // Define a value argument and add it to the command line.
        // A value arg defines a flag and a type of value that it expects,
        // such as "-n Bishop".
        ValueArg<string> logPrefixArg("p", "log-prefix", "The time stamp string to identify the test", false,
                                      "2016-04-18_18-09-00", "string");
        ValueArg<string> nameArg("f", "file", "file name (relative or absolute)", true, "file.txt", "string");
        ValueArg<string> methodArg("m", "method", "algorithm to run", true, "BFE", &allowedValues);
        ValueArg<unsigned int> sizeArg("s", "size", "flock size (mu)", true, 4, "positive integer value");
        ValueArg<double> distArg("d", "distance", "flock distance (eps)", true, 1.1, "decimal value");
        ValueArg<unsigned int> lengthArg("l", "length", "flock time length (delta)", true, 10, "positive integer value");
        SwitchArg autoChooseArg("a", "auto-method", "choose the method automatically (do not use with -m)", false);


        // Add the argument nameArg to the CmdLine object. The CmdLine object
        // uses this Arg to parse the command line.
        cmd.add(nameArg);
        cmd.add(logPrefixArg);
        cmd.add(methodArg);
        cmd.add(autoChooseArg);
        cmd.add(sizeArg);
        cmd.add(distArg);
        cmd.add(lengthArg);

        // Parse the argv array.
        cmd.parse(argc, (const char *const *) argv);

        // Do what you intend.
        string prefix = logPrefixArg.isSet() ? logPrefixArg.getValue() : "";
        runner.setAutomaticMethod(autoChooseArg.getValue());
        runner.setLogPrefix(prefix);
        runner.run(nameArg.getValue(), methodArg.getValue(), sizeArg.getValue(), distArg.getValue(),
                   lengthArg.getValue());
    } catch (TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
    }
}
