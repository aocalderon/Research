/*
Flock Evaluation Algorithms

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

#ifndef FLOCKS_FLOCKRUNNER_H
#define FLOCKS_FLOCKRUNNER_H

#include <string>
#include <vector>
#include <map>
#include "FlockBase.h"

class FlockRunner {

public:
    FlockRunner();

    std::vector<std::string> & getAllowedMethods();

    void setAutomaticMethod(bool isAutomatic);

    bool isAutomaticMethod();

    void run(std::string &inputFileName, std::string &method, unsigned int &size, double &distance, unsigned int &duration);

    void setLogPrefix(std::string &logPrefix);

private:
    std::vector<std::string> allowedMethods;

    std::map<std::string, flockcommon::FLOCKQUERYMETHOD> methodsMap;

    bool automaticMethod = false;

    unsigned long totalAnswers = 0;

    std::string logPrefix;

    void createLog(FlockBase *);
};


#endif //FLOCKS_FLOCKRUNNER_H
