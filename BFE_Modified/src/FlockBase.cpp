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

#include <FlockBase.h>


unsigned int FlockBase::getTotalAnswers() const {
    return totalAnswers;
}

unsigned int FlockBase::getTotalCenters() const {
    return totalCenters;
}

unsigned int FlockBase::getTotalClusters() const {
    return totalClusters;
}

unsigned int FlockBase::getTotalPairs() const {
    return totalPairs;
}

//==============================================================================
int FlockBase::openStream() {
    char* workingDir = getenv("WKD");

    if (workingDir != NULL){
        this->inFileName = string(workingDir) + this->inFileName;
    }

    this->fin.open(this->inFileName.c_str());
    cout << endl << "Openning stream: " << this->inFileName.c_str();

    if (!this->fin.is_open()) {
        cerr << endl << "Cannot open the file:" << this->inFileName << "\tErrno: " << strerror(errno) << endl;

        return -1; // error!
    }

    return 1;
}

//==============================================================================
int FlockBase::closeStream() {
    // Close the file
    cout << endl << "Closing stream: " << this->inFileName.c_str();
    this->fin.close();

    return 1;
}

//==============================================================================
long FlockBase::getLocation(Point *point) {
    long time_t = -1;

    // Read the one point.
    if (this->fin.good()) {
        // Read the tuple.
        fin >> point->oid;
        fin >> point->x;
        fin >> point->y;
        fin >> time_t;
        // Update the node.
        if (point->oid > this->maxOid) this->maxOid = point->oid;
        if (this->maxX < point->x) this->maxX = point->x;
        if (point->x < this->minX) this->minX = point->x;
        if (this->maxY < point->y) this->maxY = point->y;
        if (point->y < this->minY) this->minY = point->y;
    }//end if

#ifdef __DEBUGSTEPS__
	cout << endl << "t:" << time_t << " obj:" << point->oid
         << " [" << point->x << ";" << point->y << "]" << flush;
#endif
    return time_t;
}//end

//==============================================================================

/**
 * Load all the locations of a same timestamp from the data file to the myPoints field.
 *
 * @return the timestamp of the loaded locations.
 */
long FlockBase::loadLocations() {
    long time_t;
    Point point;

    // Read the dataset for a particular timestamp
    if (this->fin.good()) {
        this->myPoints.clear(); // clean points

        // get the last point
        if (this->currentTimestamp > 0) {  // test for last location.
            // add this trajectory to the structure.
            this->myPoints.insert(currentLocation);
        }//end if

        // process other points.
        while (true) {
            time_t = getLocation(&point);

            if(this->myPoints.empty())
                this->currentTimestamp = time_t;

            if (time_t == this->currentTimestamp) {
                // add this trajectory to the structure.
                this->myPoints.insert(point);
            } else {
                long oldTime = this->currentTimestamp;

                if (time_t >= 0) {
                    // stop this point to the next phase.
                    this->currentTimestamp = time_t;
                    this->currentLocation = point;
                }

                return oldTime;
            }//end if
        }//end while
    }//end while

    return -1; // some error.
}//end


bool FlockBase::createLogger(string logPrefix) {
    logPrefix = logPrefix + "_metrics.log";

    try {
        this->loggingStream.open(logPrefix, ofstream::app);
        return true;
    } catch (fstream::failure e) {
        cerr << "Error while opening the loggin stream. \n";
        cerr << "Error: " << e.what() << endl;

        return false;
    }
}

bool FlockBase::logMessage(string message) {
    try {
        if (this->loggingStream.is_open() && this->loggingStream.good()) {
            this->loggingStream << message;
            if (message.size() > 1) { // It is not \n
                this->loggingStream << "\t";
            }
        }
    } catch (ofstream::failure e) {
        cerr << "Exception while trying to write to log file.";
    }

    return false;
}

bool FlockBase::loggerIsGood() {
    return (this->loggingStream.is_open() && this->loggingStream.good());
}
