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

#ifndef FLOCKS_FLOCKANSWER_H
#define FLOCKS_FLOCKANSWER_H
//#define __PRINTANSWER__

#include <vector>
#include "HashPlaneBox.h"
#include <psb/HashCluster.h>
#include "Cluster.h"
#include "InvertedIndex.hpp"


namespace flockcommon {
    class FlockAnswer {
    public:

        FlockAnswer(const unsigned int nroFlocks = 0, const unsigned int lengthFlocks = 0) :
                lengthFlocks(lengthFlocks), nroFlocks(nroFlocks) {};


        vector<Cluster>::iterator begin() {
            return myAnswers.begin();
        }

        vector<Cluster>::iterator end() {
            return myAnswers.end();
        }

        void clear() {
            myAnswers.clear();
        }

        void shift() {
            myBoxAnswers.clear();
            myAnswers.clear();
            myIndex.reset();
            startTime++;
            endTime++;   // has to increment the endTime always!
        }

        void setLength(unsigned int len) {
            lengthFlocks = len;
        }

        void setNroFlocks(unsigned int n) {
            nroFlocks = n;
        }

        unsigned int size() {
            return (unsigned int) myAnswers.size();
        }

        unsigned int findAnswer();

        bool insertNewAnswer(Cluster flock);

        void mergeAnswer(vector<PointSet> newClusters, unsigned int time);

        void mergeAnswerIndexed(vector<HashCluster>, unsigned int);

        void mergeAnswer(vector<HashCluster> newClusters, unsigned int time);

        void mergeAnswerIndexed(vector<PointSet> newClusters, unsigned int time);

        void printAnswer(Cluster flock);

        bool isIndexed() const {
            return indexed;
        }

        void setIndexed(bool indexed) {
            FlockAnswer::indexed = indexed;
        }


        double getTimeBuildIdx() const {
            return timeBuildIdx;
        }


        long long int getClustersPruned() const {
            return clustersPruned;
        }

    private:

        vector <Cluster> myAnswers;
        vector <HashPlaneBox> myBoxAnswers;
        InvertedIndex<int, Cluster, long> myIndex;

        bool indexed = false;
        double timeBuildIdx = 0;
        long long clustersPruned = 0;
        unsigned int endTime = 0;
        unsigned int lengthFlocks = 0;
        unsigned int nroFlocks = 0;
        unsigned int startTime = 0;

        void checkDuplicateAnswer();

    };//end class
}


#endif //FLOCKS_FLOCKANSWER_H
