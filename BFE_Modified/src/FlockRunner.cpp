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

#include "FlockRunner.h"
#include "BasicFlock.h"


std::vector<std::string> &FlockRunner::getAllowedMethods() {
    return this->allowedMethods;
}

FlockRunner::FlockRunner() : allowedMethods({"BFE", "PSI"}) {
    methodsMap["BFE"] = flockcommon::BFE_METHOD;
    methodsMap["PSI"] = flockcommon::BFE_PSB_IVIDX_METHOD;
}

void FlockRunner::setAutomaticMethod(bool isAutomatic) {
    this->automaticMethod = isAutomatic;
}

bool FlockRunner::isAutomaticMethod() {
    return this->automaticMethod;
}

void FlockRunner::run(std::string &inputFileName, std::string &method,
                      unsigned int &size,
                      double &distance, unsigned int &duration) {
    if (isAutomaticMethod()) {
        // TODO(pedrotanaka): create here call for adaptive METHOD
    } else {  // Standard call
        cout << endl << "NroFlk: " << size
        << "\tLenFlk: " << duration
        << "\tDistFlk: " << distance << flush;
        switch (methodsMap[method]) {
            case BFE_METHOD: {
                cout << "\tBFE" << flush;
                BasicFlock *bfe = new BasicFlock(size, duration, distance, inputFileName);
                this->createLog(bfe);
                totalAnswers = bfe->start(BFE_METHOD);
                delete bfe;
                break;
            }
            case BFE_PSB_IVIDX_METHOD: {
                cout << "\tPSI" << flush;
                BasicFlock *psi = new BasicFlock(size, duration, distance, inputFileName);
                this->createLog(psi);
                totalAnswers = psi->start(BFE_PSB_IVIDX_METHOD);
                delete psi;
                break;
            };
        } // END - switch method

        if (totalAnswers != 0) {
            cout << " Total answers: " << totalAnswers << endl;
        }
        cout << endl << "****************************************************************" << endl << endl;
    } // end - if automatic
}

void FlockRunner::setLogPrefix(string &logPrefix) {
    this->logPrefix = logPrefix;
}

void FlockRunner::createLog(FlockBase * method) {
    if (!this->logPrefix.empty()) { // If the prefix has been set
        method->createLogger(this->logPrefix);
    }
}
